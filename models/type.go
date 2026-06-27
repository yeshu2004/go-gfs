package models

type ServerID string

type RegisterPayload struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
	Disk int64  `json:"disk"`
}

type HeartBeat struct {
	ServerID       ServerID
	TotalDiskSpace int64
	DiskUsed       int64
}

type VerfiyChunkResp struct {
	ChunkID      ChunkID              `json:"chunk_id"`
	Replicas     []ServerID           `json:"replicas"`
	ReplicaAddrs map[ServerID]string  `json:"replica_addrs"`
}

type ChunkID string

type FileMetadata struct {
	Filename string
	Chunks   []ChunkID
}
type ChunkLocation struct {
	ChunkID ChunkID
	Servers []ServerID
}
