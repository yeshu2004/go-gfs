package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/gogo/protobuf/proto"
)


// ProtoMessage implements [proto.Message]. It is a marker method.
func (w *WAL_Entry) ProtoMessage() {}

// Reset implements [proto.Message]. It zeroes the entry.
func (w *WAL_Entry) Reset() {
	*w = WAL_Entry{}
}

// String implements [proto.Message].
func (w *WAL_Entry) String() string {
	return proto.CompactTextString(w)
}

func MustMarshal(entry *WAL_Entry) []byte {
	data, err := proto.Marshal(entry)
	if err != nil {
		panic(fmt.Sprintf("proto marshal error: %v", err))
	}
	return data
}

func MustUnmarshal(buff []byte, entry *WAL_Entry) error {
	return proto.Unmarshal(buff, entry)
}


func UnmarshalAndVerifyCheckSum(data []byte) (*WAL_Entry, error){
	var entry WAL_Entry;

	if err := MustUnmarshal(data, &entry); err != nil{
		return nil, err;
	}

	if !verifyCRC(&entry){
		return nil, fmt.Errorf("CRC mismatch: data may be corrupted")
	}

	return &entry, nil;
}

func computeCRC(data []byte, sequenceNo uint64) uint32 {
	seqBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(seqBytes, sequenceNo)
	return crc32.ChecksumIEEE(append(data, seqBytes...))
}

func verifyCRC(entry *WAL_Entry) bool {
	return computeCRC(entry.Data, entry.SequenceNo) == entry.CRC
}