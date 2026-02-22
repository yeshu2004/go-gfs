package wal

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	syncInterval  = 200 * time.Millisecond
	segmentPrefix = "segment-"
	maxFileSize   = 64 * 1024 * 1024 // chunk size
)

type WAL_Entry struct {
	SequenceNo uint64 `protobuf:"varint,1,opt,name=sequence_no,json=sequenceNo,proto3" json:"sequence_no,omitempty"`
	Data       []byte `protobuf:"bytes,2,opt,name=data,proto3"                          json:"data,omitempty"`
	CRC        uint32 `protobuf:"varint,3,opt,name=crc,proto3"                          json:"crc,omitempty"`
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
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	files, err := filepath.Glob(filepath.Join(dir, segmentPrefix+"*"))
	if err != nil {
		return nil, err
	}

	lastSegmentID := 0
	var lastSequenceNo uint64

	if len(files) > 0 {
		lastSegmentID, err = findLastSegmentFileIndex(files)
		if err != nil {
			return nil, err
		}

		// recover lastSequenceNo by scanning all segments in order.
		lastSequenceNo, err = recoverLastSequenceIndex(dir, files)
		if err != nil {
			return nil, err
		}
	} else {
		// no segments yet, create the initial one.
		file, err := createSegmentFile(dir, lastSegmentID)
		if err != nil {
			return nil, err
		}
		if err := file.Close(); err != nil {
			return nil, err
		}
	}

	filePath := filepath.Join(dir, fmt.Sprintf("%s%d", segmentPrefix, lastSegmentID))
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	wal := &WAL{
		directory:           dir,
		currentSegment:      file,
		lastSequenceNo:      lastSequenceNo,
		bufWriter:           bufio.NewWriter(file),
		syncTimer:           time.NewTimer(syncInterval),
		shouldFsync:         shouldFsync,
		maxFileSize:         maxFileSize,
		maxSegments:         10,
		currentSegmentIndex: lastSegmentID,
		ctx:                 ctx,
		cancel:              cancel,
	}

	go wal.syncLoop()

	return wal, nil
}

// Write appends data to the WAL and returns any error.
func (w *WAL) Write(data []byte) error {
	return w.writeEntry(data)
}

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
		CRC:        computeCRC(data, w.lastSequenceNo),
	}

	marshaledEntry := MustMarshal(entry)

	size := int32(len(marshaledEntry))
	if err := binary.Write(w.bufWriter, binary.LittleEndian, size); err != nil {
		return err
	}

	_, err := w.bufWriter.Write(marshaledEntry)
	return err
}

func (w *WAL) rotateLogIfNeeded() error {
	fileInfo, err := w.currentSegment.Stat()
	if err != nil {
		return err
	}

	if fileInfo.Size()+int64(w.bufWriter.Buffered()) >= w.maxFileSize {
		return w.rotateLog()
	}

	return nil
}

func (w *WAL) rotateLog() error {
	if err := w.sync(); err != nil {
		return err
	}

	if err := w.currentSegment.Close(); err != nil {
		return err 
	}

	w.currentSegmentIndex++

	if w.currentSegmentIndex > w.maxSegments {
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

	if len(files) == 0 {
		return nil
	}

	oldestSegmentFile, err := w.findOldestSegmentFile(files)
	if err != nil {
		return err
	}

	return os.Remove(oldestSegmentFile)
}

func (w *WAL) ReadAll() ([]*WAL_Entry, error) {
	files, err := filepath.Glob(filepath.Join(w.directory, segmentPrefix+"*"))
	if err != nil {
		return nil, err
	}

	// sort by segment index so entries are returned in write order.
	sortSegmentFiles(files, w.directory)

	var allEntries []*WAL_Entry
	for _, path := range files {
		f, err := os.OpenFile(path, os.O_RDONLY, 0644)
		if err != nil {
			return nil, err
		}

		entries, _, err := readEntrieFromFile(f)
		f.Close()
		if err != nil {
			return nil, err
		}

		allEntries = append(allEntries, entries...)
	}

	return allEntries, nil
}


func (w *WAL) syncLoop() {
	ticker := time.NewTicker(syncInterval)
	for {
		select {
		case <-ticker.C:
			w.lock.Lock()
			err := w.sync()
			w.lock.Unlock()

			if err != nil {
				log.Printf("error while performing sync: %v", err)
				w.syncTimer.Reset(syncInterval)
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
	w.syncTimer.Reset(syncInterval)
	return nil
}

// Close flushes and syncs the WAL then releases all resources.
func (w *WAL) Close() error {
	w.cancel()

	w.lock.Lock()
	defer w.lock.Unlock()

	if err := w.sync(); err != nil {
		return err
	}

	return w.currentSegment.Close()
}

// readEntrieFromFile reads all entries from a single segment file.
func readEntrieFromFile(file *os.File) ([]*WAL_Entry, uint64, error) {
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
		if _, err := io.ReadFull(file, data); err != nil {
			return nil, checkpointLogSequenceNo, err
		}

		entry, err := UnmarshalAndVerifyCheckSum(data)
		if err != nil {
			return nil, checkpointLogSequenceNo, err
		}

		checkpointLogSequenceNo = entry.SequenceNo
		entries = append(entries, entry)
	}

	return entries, checkpointLogSequenceNo, nil
}

// recoverLastSequenceNo scans all segment files in order and returns
// the highest sequence number found.
// called once during OpenWAL so that the WAL resumes from where it left 
// off after a restart.
func recoverLastSequenceIndex(dir string, files []string) (uint64, error) {
	sortSegmentFiles(files, dir)

	var lastSeq uint64
	for _, path := range files {
		f, err := os.OpenFile(path, os.O_RDONLY, 0644)
		if err != nil {
			return 0, err
		}

		_, seq, err := readEntrieFromFile(f)
		f.Close()
		if err != nil {
			return 0, err
		}

		if seq > lastSeq {
			lastSeq = seq
		}
	}

	return lastSeq, nil
}

// sortSegmentFiles sorts file paths by their numeric segment index in
// ascending order so that entries are processed in write order.
func sortSegmentFiles(files []string, dir string) {
	sort.Slice(files, func(i, j int) bool {
		idxI, _ := strconv.Atoi(strings.TrimPrefix(filepath.Base(files[i]), segmentPrefix))
		idxJ, _ := strconv.Atoi(strings.TrimPrefix(filepath.Base(files[j]), segmentPrefix))
		return idxI < idxJ
	})
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

func findLastSegmentFileIndex(files []string) (int, error) {
	var lastSegmentID int
	for _, file := range files {
		_, fileName := filepath.Split(file)
		segmentID, err := strconv.Atoi(strings.TrimPrefix(fileName, segmentPrefix))
		if err != nil {
			return 0, err
		}
		if segmentID > lastSegmentID {
			lastSegmentID = segmentID
		}
	}

	return lastSegmentID, nil
}

func createSegmentFile(dir string, segmentID int) (*os.File, error) {
	filePath := filepath.Join(dir, fmt.Sprintf("%s%d", segmentPrefix, segmentID))
	return os.Create(filePath)
}