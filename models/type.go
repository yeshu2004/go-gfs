package models

import "time"

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
	ChunkID      ChunkID             `json:"chunk_id"`
	Replicas     []ServerID          `json:"replicas"`
	ReplicaAddrs map[ServerID]string `json:"replica_addrs"`
}

type ChunkID string

type FileMetadata struct {
	Filename string
	Chunks   []ChunkID
}

type FileMetaData struct {
	FileID    string    `json:"file_id"`
	FileName  string    `json:"file_name"`
	FileType  string    `json:"file_type"`
	Size      int64     `json:"size"`
	ChunkIDs  []ChunkID `json:"chunk_ids"`
	CreatedAt time.Time `json:"created_at"`
	Status    string    `json:"status"` // "pending" | "committed" | "failed"
}

type ChunkLocation struct {
	ChunkID     ChunkID    `json:"chunk_id"`
	Primary     ServerID   `json:"primary_server_name"`
	PrimaryAddr string     `json:"primary_addr"`
	Replicas    []ServerID `json:"replica_servers"`
}

type FileInfoResponse struct {
	FileID    string          `json:"file_id"`
	FileName  string          `json:"file_name"`
	FileType  string          `json:"file_type"`
	Size      int64           `json:"size"`
	CreatedAt time.Time       `json:"created_at"`
	Chunks    []ChunkLocation `json:"chunks"` // ordered — same order the file was written in
	Status    string    `json:"status"` // "pending" | "committed" | "failed"
}


type ReplicateChunkResponse struct {
	Checksum string `json:"checksum"`
	Size     int64  `json:"size"`
}