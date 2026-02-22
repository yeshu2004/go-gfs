package wal

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	syncInterval  = 200 * time.Millisecond
	segmentPrefix = "segment-"
	maxFileSize   = 64 * 1024 * 1024
)

type WAL_Entry struct {
	SequenceNo uint64
	Data       []byte
	CRC        uint32 // checksum for corruption detection
}

type WAL struct {
	directory           string
	currentSegment      *os.File
	lock                sync.Mutex
	lastSequenceNo      uint64
	bufWriter           *bufio.Writer
	syncTimer           *time.Timer
	shouldFsync         bool
	maxFileSize         int64
	maxSegments         int
	currentSegmentIndex int
	ctx                 context.Context
	cancel              context.CancelFunc
}

func OpenWAL(dir string, shouldFsync bool) (*WAL, error) {
	// 1. create a dir / open a dir
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	// read the log segement
	files, err := filepath.Glob(filepath.Join(dir, segmentPrefix+"*"))
	if err != nil {
		return nil, err
	}

	lastSegmentID := 0
	if len(files) > 0 {
		// find the last segmentID
		lastSegmentID, err = findLastSegmentFileIndex(files)
		if err != nil {
			return nil, err
		}
	} else {
		// if log segement doesn't exists, create new one
		file, err := createSegmentFile(dir, lastSegmentID)
		if err != nil {
			return nil, err
		}

		if err := file.Close(); err != nil {
			return nil, err
		}
	}

	// open the last log segment file
	filePath := filepath.Join(dir, fmt.Sprintf("%s%d", segmentPrefix, lastSegmentID))
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	// go to the end of the file for append only nature
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	wal := &WAL{
		directory:           dir,
		currentSegment:      file,
		lastSequenceNo:      0,
		bufWriter:           bufio.NewWriter(file),
		syncTimer:           time.NewTimer(syncInterval),
		shouldFsync:         shouldFsync, // true or false
		maxFileSize:         maxFileSize, // chunk size i.e. 64MB
		maxSegments:         10,
		currentSegmentIndex: lastSegmentID,
		ctx:                 ctx,
		cancel:              cancel,
	}

	go wal.syncLoop()

	return wal, nil
}

func (w *WAL) Write(data []byte) error {
	return w.writeEntry(data)
}

// main logic behind write from backend client
func (w *WAL) writeEntry(data []byte) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	if err := w.rotateLogIfNeeded(); err != nil {
		return err
	}

	w.lastSequenceNo++
	entry := &WAL_Entry{
		SequenceNo: w.lastSequenceNo,
		Data:       data,
		CRC:        crc32.ChecksumIEEE(append(data, byte(w.lastSequenceNo))),
	}

	marshedEntry := MustMarshal(entry)
	size := int32(len(marshedEntry))

	if err := binary.Write(w.bufWriter, binary.LittleEndian, size); err != nil {
		return err
	}

	_, err := w.bufWriter.Write(marshedEntry)
	return err
}

func (w *WAL) rotateLogIfNeeded() error {
	fileInfo, err := w.currentSegment.Stat()
	if err != nil {
		return err
	}

	if fileInfo.Size()+int64(w.bufWriter.Buffered()) >= w.maxFileSize {
		// rotate log
		if err := w.rotateLog(); err != nil {
			return err
		}
	}

	return nil
}

func (w *WAL) rotateLog() error {
	// first sync all data
	if err := w.sync(); err != nil {
		return err
	}

	if err := w.currentSegment.Close(); err != nil {
		return err
	}

	w.currentSegmentIndex++
	if w.currentSegmentIndex >= w.maxSegments {
		// delete old one
		if err := w.deleteOldSegment(); err != nil {
			return err
		}
	}

	newSegment, err := createSegmentFile(w.directory, w.currentSegmentIndex)
	if err != nil {
		return err
	}

	w.currentSegment = newSegment
	w.bufWriter = bufio.NewWriter(newSegment)
	return nil
}

func (w *WAL) deleteOldSegment() error {
	files, err := filepath.Glob(filepath.Join(w.directory, segmentPrefix+"*"))
	if err != nil {
		return err
	}

	var oldestSegmentFile string
	if len(files) > 0 {
		// find oldest segment file path
		oldestSegmentFile, err = w.findOldestSegmentFile(files)
		if err != nil {
			return err
		}
	} else {
		return nil
	}

	if err := os.Remove(oldestSegmentFile); err != nil {
		return err
	}

	return nil
}

func (w *WAL) ReadAll() ([]*WAL_Entry, error) {
	file, err := os.OpenFile(w.currentSegment.Name(), os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	entries, _, err := readEntriesFromFile(file)
	if err != nil {
		return nil, err
	}

	return entries, nil
}

func readEntriesFromFile(file *os.File) ([]*WAL_Entry, uint64, error) {
	var entries []*WAL_Entry
	checkpointLogSequenceNo := uint64(0)
	for {
		var size int32
		if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
			if err == io.EOF {
				break 
			}
			return nil, checkpointLogSequenceNo, err
		}

		data := make([]byte, size)
		n, err := io.ReadFull(file, data)
		if err != nil {
			return nil, checkpointLogSequenceNo, err
		}
		log.Printf("read (%d) bytes from the segement file - %v", n, file)

		entry, err := UnmarshalAndVerifyCheckSum(data)
		if err != nil {
			return nil, checkpointLogSequenceNo, err
		}
		checkpointLogSequenceNo = entry.SequenceNo
		entries = append(entries, entry)
	}

	return entries, checkpointLogSequenceNo, nil
}

func (w *WAL) findOldestSegmentFile(files []string) (string, error) {
	var oldestSegmentPath string
	oldestIDSegment := math.MaxInt64

	for _, file := range files {
		segmentIndex, err := strconv.Atoi(strings.TrimPrefix(file, filepath.Join(w.directory, segmentPrefix)))
		if err != nil {
			return "", err
		}

		if segmentIndex < oldestIDSegment {
			oldestIDSegment = segmentIndex
			oldestSegmentPath = file
		}
	}

	return oldestSegmentPath, nil
}

func (w *WAL) syncLoop() {
	for {
		select {
		case <-w.syncTimer.C:
			w.lock.Lock()
			err := w.sync()
			w.lock.Unlock()

			if err != nil {
				log.Printf("error while performing sync: %v", err)
			}
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *WAL) sync() error {
	if err := w.bufWriter.Flush(); err != nil {
		return err
	}
	if w.shouldFsync {
		if err := w.currentSegment.Sync(); err != nil {
			return err
		}
	}
	w.syncTimer.Reset(syncInterval) // reset the keep syncing timer
	return nil
}

func findLastSegmentFileIndex(files []string) (int, error) {
	var lastSegementID int
	for _, file := range files {
		_, fileName := filepath.Split(file)
		segmentID, err := strconv.Atoi(strings.TrimPrefix(fileName, segmentPrefix))
		if err != nil {
			return 0, err
		}
		if segmentID > lastSegementID {
			lastSegementID = segmentID
		}
	}

	return lastSegementID, nil
}

func createSegmentFile(dir string, segmentID int) (*os.File, error) {
	filePath := filepath.Join(dir, fmt.Sprintf("%s%d", segmentPrefix, segmentID))
	return os.Create(filePath)
}
