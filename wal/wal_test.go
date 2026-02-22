// full ai test written :)

package wal

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func openTestWAL(t *testing.T) (*WAL, string) {
	t.Helper()
	dir := t.TempDir()
	w, err := OpenWAL(dir, false)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w, dir
}

func TestOpenWAL_CreatesSegmentFile(t *testing.T) {
	_, dir := openTestWAL(t)

	matches, err := filepath.Glob(filepath.Join(dir, segmentPrefix+"*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) == 0 {
		t.Fatal("expected at least one segment file to be created")
	}
}

func TestOpenWAL_ExistingDirectory(t *testing.T) {
	_, dir := openTestWAL(t)

	w2, err := OpenWAL(dir, false)
	if err != nil {
		t.Fatalf("second OpenWAL: %v", err)
	}
	_ = w2.Close()
}

// -------------------------------------------------------------------
// Write
// -------------------------------------------------------------------

func TestWrite_SingleEntry(t *testing.T) {
	w, _ := openTestWAL(t)

	if err := w.Write([]byte("hello")); err != nil {
		t.Fatalf("Write: %v", err)
	}
}

func TestWrite_ReturnsError_OnNilData(t *testing.T) {
	// Writing nil should not panic — it is valid protobuf bytes content.
	w, _ := openTestWAL(t)
	if err := w.Write(nil); err != nil {
		t.Fatalf("Write(nil): unexpected error: %v", err)
	}
}

func TestWrite_SequenceNumbersAreMonotonic(t *testing.T) {
	w, _ := openTestWAL(t)

	for i := 0; i < 5; i++ {
		if err := w.Write([]byte(fmt.Sprintf("entry-%d", i))); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	if err := w.sync(); err != nil {
		t.Fatal(err)
	}

	entries, err := w.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	for i, e := range entries {
		want := uint64(i + 1)
		if e.SequenceNo != want {
			t.Errorf("entry[%d]: SequenceNo = %d, want %d", i, e.SequenceNo, want)
		}
	}
}

// -------------------------------------------------------------------
// ReadAll
// -------------------------------------------------------------------

func TestReadAll_RoundTrip(t *testing.T) {
	w, _ := openTestWAL(t)

	payloads := []string{"alpha", "beta", "gamma"}
	for _, p := range payloads {
		if err := w.Write([]byte(p)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.sync(); err != nil {
		t.Fatal(err)
	}

	entries, err := w.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(entries) != len(payloads) {
		t.Fatalf("got %d entries, want %d", len(entries), len(payloads))
	}
	for i, e := range entries {
		if string(e.Data) != payloads[i] {
			t.Errorf("entry[%d]: data = %q, want %q", i, e.Data, payloads[i])
		}
	}
}

func TestReadAll_EmptyWAL(t *testing.T) {
	w, _ := openTestWAL(t)

	entries, err := w.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll on empty WAL: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(entries))
	}
}

// -------------------------------------------------------------------
// Sequence number recovery across restarts (key regression)
// -------------------------------------------------------------------

func TestSequenceNoRecoveredAcrossRestart(t *testing.T) {
	dir := t.TempDir()

	// First session: write three entries.
	w1, err := OpenWAL(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := w1.Write([]byte("data")); err != nil {
			t.Fatal(err)
		}
	}
	if err := w1.Close(); err != nil {
		t.Fatal(err)
	}

	// Second session: open the same dir, write one more entry.
	w2, err := OpenWAL(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	if err := w2.Write([]byte("after restart")); err != nil {
		t.Fatal(err)
	}
	if err := w2.sync(); err != nil {
		t.Fatal(err)
	}

	// Sequence numbers must be 1, 2, 3, 4 — not 1, 2, 3, 1.
	entries, err := w2.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 4 {
		t.Fatalf("want 4 entries, got %d", len(entries))
	}
	for i, e := range entries {
		want := uint64(i + 1)
		if e.SequenceNo != want {
			t.Errorf("entry[%d]: SequenceNo = %d, want %d (not monotonic across restart)", i, e.SequenceNo, want)
		}
	}
}

// -------------------------------------------------------------------
// CRC / corruption detection
// -------------------------------------------------------------------

func TestCRC_DetectsCorruption(t *testing.T) {
	w, _ := openTestWAL(t)

	if err := w.Write([]byte("important")); err != nil {
		t.Fatal(err)
	}
	if err := w.sync(); err != nil {
		t.Fatal(err)
	}

	// Flip a byte in the segment file to simulate corruption.
	files, _ := filepath.Glob(filepath.Join(w.directory, segmentPrefix+"*"))
	raw, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	// The first 4 bytes are the int32 length prefix; corrupt a data byte after that.
	if len(raw) > 10 {
		raw[10] ^= 0xFF
	}
	if err := os.WriteFile(files[0], raw, 0644); err != nil {
		t.Fatal(err)
	}

	_, err = w.ReadAll()
	if err == nil {
		t.Fatal("expected error from corrupted data, got nil")
	}
}

func TestComputeCRC_NoDuplicatesAcross256Boundary(t *testing.T) {
	// With the old byte(sequenceNo) truncation, seqNo 1 and 257 would
	// produce the same CRC for the same data. Verify this is fixed.
	data := []byte("payload")
	crc1 := computeCRC(data, 1)
	crc257 := computeCRC(data, 257)

	if crc1 == crc257 {
		t.Error("CRC collision: sequence numbers 1 and 257 produced the same checksum")
	}
}

func TestVerifyCRC_ValidEntry(t *testing.T) {
	entry := &WAL_Entry{
		SequenceNo: 42,
		Data:       []byte("test"),
	}
	entry.CRC = computeCRC(entry.Data, entry.SequenceNo)

	if !verifyCRC(entry) {
		t.Error("verifyCRC returned false for a valid entry")
	}
}

func TestVerifyCRC_TamperedData(t *testing.T) {
	entry := &WAL_Entry{
		SequenceNo: 1,
		Data:       []byte("original"),
	}
	entry.CRC = computeCRC(entry.Data, entry.SequenceNo)

	entry.Data = []byte("tampered")
	if verifyCRC(entry) {
		t.Error("verifyCRC returned true after data was tampered")
	}
}

// -------------------------------------------------------------------
// Marshal / Unmarshal
// -------------------------------------------------------------------

func TestMustMarshal_PanicsOnNilEntry(t *testing.T) {
	// A nil *WAL_Entry pointer should cause MustMarshal to panic.
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected MustMarshal(nil) to panic")
		}
	}()
	MustMarshal(nil)
}

func TestMarshalUnmarshalRoundTrip(t *testing.T) {
	original := &WAL_Entry{
		SequenceNo: 99,
		Data:       []byte("roundtrip"),
		CRC:        computeCRC([]byte("roundtrip"), 99),
	}

	encoded := MustMarshal(original)
	if encoded == nil {
		t.Fatal("MustMarshal returned nil")
	}

	var decoded WAL_Entry
	if err := MustUnmarshal(encoded, &decoded); err != nil {
		t.Fatalf("MustUnmarshal: %v", err)
	}

	if decoded.SequenceNo != original.SequenceNo {
		t.Errorf("SequenceNo: got %d, want %d", decoded.SequenceNo, original.SequenceNo)
	}
	if string(decoded.Data) != string(original.Data) {
		t.Errorf("Data: got %q, want %q", decoded.Data, original.Data)
	}
	if decoded.CRC != original.CRC {
		t.Errorf("CRC: got %d, want %d", decoded.CRC, original.CRC)
	}
}

// -------------------------------------------------------------------
// Segment rotation
// -------------------------------------------------------------------

func TestRotation_NewSegmentCreated(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// Shrink maxFileSize so rotation happens quickly.
	w.maxFileSize = 100

	// Write until rotation occurs (at least two segment files exist).
	for i := 0; i < 200; i++ {
		if err := w.Write([]byte(fmt.Sprintf("entry-%04d", i))); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	if err := w.sync(); err != nil {
		t.Fatal(err)
	}

	files, _ := filepath.Glob(filepath.Join(dir, segmentPrefix+"*"))
	if len(files) < 2 {
		t.Fatalf("expected at least 2 segment files after rotation, got %d", len(files))
	}
}

func TestRotation_ReadAllSpansSegments(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	w.maxFileSize = 100

	const n = 50
	for i := 0; i < n; i++ {
		if err := w.Write([]byte(fmt.Sprintf("msg-%d", i))); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	if err := w.sync(); err != nil {
		t.Fatal(err)
	}

	entries, err := w.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(entries) != n {
		t.Fatalf("got %d entries across segments, want %d", len(entries), n)
	}
	// Verify order is preserved across segment boundaries.
	for i, e := range entries {
		want := uint64(i + 1)
		if e.SequenceNo != want {
			t.Errorf("entry[%d]: SequenceNo = %d, want %d", i, e.SequenceNo, want)
		}
	}
}

func TestRotation_OldSegmentsDeleted(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	w.maxFileSize = 50
	w.maxSegments = 3

	// Write enough to exceed maxSegments rotations.
	for i := 0; i < 500; i++ {
		if err := w.Write([]byte(fmt.Sprintf("x-%d", i))); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	if err := w.sync(); err != nil {
		t.Fatal(err)
	}

	files, _ := filepath.Glob(filepath.Join(dir, segmentPrefix+"*"))
	if len(files) > w.maxSegments+1 {
		t.Errorf("too many segment files: got %d, maxSegments = %d", len(files), w.maxSegments)
	}
}

// -------------------------------------------------------------------
// Concurrent writes
// -------------------------------------------------------------------

func TestWrite_ConcurrentSafety(t *testing.T) {
	w, _ := openTestWAL(t)

	const goroutines = 10
	const writesEach = 20

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := 0; i < writesEach; i++ {
				if err := w.Write([]byte(fmt.Sprintf("g%d-i%d", g, i))); err != nil {
					t.Errorf("Write: %v", err)
				}
			}
		}()
	}
	wg.Wait()

	if err := w.sync(); err != nil {
		t.Fatal(err)
	}

	entries, err := w.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != goroutines*writesEach {
		t.Errorf("got %d entries, want %d", len(entries), goroutines*writesEach)
	}
}

// -------------------------------------------------------------------
// Close
// -------------------------------------------------------------------

func TestClose_FlushesData(t *testing.T) {
	dir := t.TempDir()

	w, err := OpenWAL(dir, false)
	if err != nil {
		t.Fatal(err)
	}

	if err := w.Write([]byte("before-close")); err != nil {
		t.Fatal(err)
	}
	// Close must flush the buffer before we re-open.
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := OpenWAL(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	entries, err := w2.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || string(entries[0].Data) != "before-close" {
		t.Errorf("unexpected entries after reopen: %v", entries)
	}
}

// -------------------------------------------------------------------
// Binary encoding width (int32 write / int32 read consistency)
// -------------------------------------------------------------------

func TestBinaryEncoding_SizeFieldIsInt32(t *testing.T) {
	// Write an entry manually and verify the size prefix is 4 bytes wide.
	dir := t.TempDir()
	w, err := OpenWAL(dir, false)
	if err != nil {
		t.Fatal(err)
	}

	if err := w.Write([]byte("sizecheck")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	files, _ := filepath.Glob(filepath.Join(dir, segmentPrefix+"*"))
	raw, err := os.ReadFile(files[len(files)-1])
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) < 4 {
		t.Fatal("segment file too small to contain an int32 prefix")
	}

	// Read the first 4 bytes as int32 and confirm the remainder of the
	// file is exactly that many bytes long.
	size := int32(binary.LittleEndian.Uint32(raw[:4]))
	if int(size) != len(raw)-4 {
		t.Errorf("size prefix = %d, remaining bytes = %d (mismatch)", size, len(raw)-4)
	}
}

// -------------------------------------------------------------------
// WAL_Entry proto methods
// -------------------------------------------------------------------

func TestWALEntry_Reset(t *testing.T) {
	e := &WAL_Entry{SequenceNo: 7, Data: []byte("x"), CRC: 123}
	e.Reset()
	if e.SequenceNo != 0 || e.Data != nil || e.CRC != 0 {
		t.Errorf("Reset did not zero the entry: %+v", e)
	}
}

func TestWALEntry_ProtoMessage_DoesNotPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("ProtoMessage panicked: %v", r)
		}
	}()
	e := &WAL_Entry{}
	e.ProtoMessage()
}

func TestWALEntry_String_NotEmpty(t *testing.T) {
	e := &WAL_Entry{SequenceNo: 1, Data: []byte("hi")}
	if e.String() == "" {
		t.Error("String() returned empty string")
	}
}
