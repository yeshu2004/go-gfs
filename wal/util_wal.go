package wal

import (
	"fmt"
	"hash/crc32"
	"log"

	"github.com/gogo/protobuf/proto"
)

// ProtoMessage implements [proto.Message].
func (w *WAL_Entry) ProtoMessage() {
	panic("unimplemented")
}

// Reset implements [proto.Message].
func (w *WAL_Entry) Reset() {
	panic("unimplemented")
}

// String implements [proto.Message].
func (w *WAL_Entry) String() string {
	panic("unimplemented")
}


func MustMarshal(entry *WAL_Entry) []byte{
	marshalEntry, err := proto.Marshal(entry);
	if err != nil{
		log.Printf("proto marshal error: %v", err);
		return nil;
	}
	return marshalEntry;
}

func MustUnmarshal(buff []byte, entry *WAL_Entry) error {
	return proto.Unmarshal(buff, entry)
}


func UnmarshalAndVerifyCheckSum(data []byte) (*WAL_Entry, error){
	var entry WAL_Entry;

	if err := MustUnmarshal(data, &entry); err != nil{
		return nil, err;
	}

	if !vefiryCRC(&entry){
		return nil, fmt.Errorf("CRC mismatch: data may be corrupted")
	}

	return &entry, nil;
}

func vefiryCRC(entry *WAL_Entry) bool{
	actualCRC := crc32.ChecksumIEEE(append(entry.Data, byte(entry.SequenceNo)))

	return actualCRC == entry.CRC;
}