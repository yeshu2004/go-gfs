package wal

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
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
    CRC        uint32    // checksum for corruption detection
    CreatedAt  time.Time
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
		lastSegmentID, err = findLastSegmentIndexFile(files)
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

	go wal.syncLoop();

	return wal, nil
}

func (w *WAL) syncLoop() {
	for {
		select {
		case <-w.syncTimer.C:
			w.lock.Lock()
			err := w.sync()
			w.lock.Unlock()

			if err != nil{
				log.Printf("error while performing sync: %v", err)
			}
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *WAL) sync() error {
	if err := w.bufWriter.Flush(); err != nil {
		return err;
	}
	if w.shouldFsync {
		if err := w.currentSegment.Sync(); err != nil{
			return err;
		}
	}
	w.syncTimer.Reset(syncInterval) // reset the keep syncing timer
	return nil;
}

func findLastSegmentIndexFile(files []string) (int, error) {
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
