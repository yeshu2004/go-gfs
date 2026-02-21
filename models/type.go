package models


type ServerID string

type RegisterPayload struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
	Disk int64  `json:"disk"`
}

type HeartBeat struct{
	ServerID ServerID
	DiskSpace int64
}

type ChunkID string

type FileMetadata struct {
    Filename string
    Chunks   []ChunkID
}
type ChunkLocation struct {
    ChunkID   ChunkID
    Servers   []ServerID
}
