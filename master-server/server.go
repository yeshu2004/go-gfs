package masterserver

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/yeshu2004/gfs/models"
)

const (
	ChunkSize         int64 = 64 * 1024 * 1024
	ReplicationFactor       = 3
)

type ChunkMeta struct {
	Addr         string
	TotalDisk    int64
	PhysicalUsed int64 // actual disk usage from heartbeat
	ReservedDisk int64 // logical reservations from master
}

type MasterServer struct {
	listenAddr string
	heartbeats map[models.ServerID]time.Time
	chunkMeta  map[models.ServerID]ChunkMeta
	mu         sync.RWMutex

	fileToChunks  map[string][]models.ChunkID
	pendingChunks map[models.ChunkID][]models.ServerID // temprory state
	chunkToServer map[models.ChunkID][]models.ServerID // update: after commit to disk
}

func NewMasterServer(listenAddr string) *MasterServer {
	return &MasterServer{
		listenAddr:    listenAddr,
		heartbeats:    make(map[models.ServerID]time.Time),
		chunkMeta:     make(map[models.ServerID]ChunkMeta),
		fileToChunks:  make(map[string][]models.ChunkID),
		pendingChunks: make(map[models.ChunkID][]models.ServerID),
		chunkToServer: make(map[models.ChunkID][]models.ServerID),
	}
}

func (m *MasterServer) RunServer() error {
	mux := http.NewServeMux()

	mux.HandleFunc("/register", m.registerChunkServerHandler) // WORKING
	mux.HandleFunc("/heartbeat", m.heartBeatsHandler)         // WORKING
	mux.HandleFunc("/chunk-server", m.fileMappingHandler)     // WORKING

	go m.monitorHeartbeats()

	log.Printf("master server about to listen on %s", m.listenAddr)
	return http.ListenAndServe(m.listenAddr, mux)
}

// TODO: to figure out fix when same file name exists for two diffent files
func (m *MasterServer) fileMappingHandler(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(rw, fmt.Sprintf("%v not allowed", r.Method), http.StatusMethodNotAllowed)
		return
	}
	log.Printf("%s methord recived at %s", r.Method, r.URL.Path)

	// 0. recive filename along with every chunk
	type reqBody struct {
		FileName string `json:"file_name"`
	}

	var pl reqBody
	if err := json.NewDecoder(r.Body).Decode(&pl); err != nil {
		http.Error(rw, "invalid request", http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	// 1. ensure file entry exists
	if _, exists := m.fileToChunks[pl.FileName]; !exists {
		m.fileToChunks[pl.FileName] = []models.ChunkID{}
	}

	// 2. find healty & eligible servers
	var eligibleServers []models.ServerID
	for srv, _ := range m.heartbeats {
		meta := m.chunkMeta[srv]
		avail := meta.TotalDisk - meta.PhysicalUsed - meta.ReservedDisk
		if avail >= ChunkSize {
			eligibleServers = append(eligibleServers, srv)
		}
	}

	if len(eligibleServers) < ReplicationFactor {
		http.Error(rw, "not enough servers with space", http.StatusServiceUnavailable)
		return
	}

	// 3. shuffle among the eligible servers
	rand.Shuffle(len(eligibleServers), func(i, j int) {
		eligibleServers[i], eligibleServers[j] = eligibleServers[j], eligibleServers[i]
	})
	selectedServer := eligibleServers[:ReplicationFactor]

	// 4. genrate chunkID
	// have to look for better small id bcz uuid are 128 bit,
	// paper suggests for 64 bit bcz for memory efficiency
	chunkId := uuid.New().String(); 

	// 4. substract the available space for those server for now
	for _, server := range selectedServer {
		meta := m.chunkMeta[server]
		meta.ReservedDisk += ChunkSize
		m.chunkMeta[server] = meta
	}

	// 5. update fileToChunks & chunkToServer
	m.fileToChunks[pl.FileName] = append(m.fileToChunks[pl.FileName], models.ChunkID(chunkId))
	if _, exists := m.pendingChunks[models.ChunkID(chunkId)]; !exists {
		m.pendingChunks[models.ChunkID(chunkId)] = []models.ServerID{}
	}
	m.pendingChunks[models.ChunkID(chunkId)] = selectedServer

	type respBody struct {
		ChunkID models.ChunkID    `json:"chunk_id"`
		Servers []models.ServerID `json:"servers"`
	}

	// 6. retrun back the chunkID & servers to client
	rw.Header().Set("Content-Type", "application/json")
	json.NewEncoder(rw).Encode(respBody{
		ChunkID: models.ChunkID(chunkId),
		Servers: selectedServer,
	})
}

func (m *MasterServer) registerChunkServerHandler(rw http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		rw.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var pl models.RegisterPayload
	if err := json.NewDecoder(req.Body).Decode(&pl); err != nil {
		rw.WriteHeader(http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	m.chunkMeta[models.ServerID(pl.ID)] = ChunkMeta{
		Addr:         pl.Addr,
		TotalDisk:    pl.Disk,
		PhysicalUsed: 0,
		ReservedDisk: 0,
	}
	m.heartbeats[models.ServerID(pl.ID)] = time.Now()
	m.mu.Unlock()

	log.Printf("registered chunk server (%s) at %s", pl.ID, pl.Addr)

	rw.WriteHeader(http.StatusOK)
}

func (m *MasterServer) heartBeatsHandler(rw http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		rw.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var pl models.HeartBeat
	if err := json.NewDecoder(req.Body).Decode(&pl); err != nil {
		rw.WriteHeader(http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	m.heartbeats[pl.ServerID] = time.Now()
	meta := m.chunkMeta[pl.ServerID]
	meta.PhysicalUsed = pl.DiskUsed
	m.chunkMeta[pl.ServerID] = meta
	m.mu.Unlock()

	log.Printf("heartbeat recived - server (%s) is ALIVE with disk space: %d", pl.ServerID, pl.TotalDiskSpace-pl.DiskUsed)
	rw.WriteHeader(http.StatusOK)
}

// TODO: will be done....
// Updates ChunkToServer map from pendingChunk Map & deletes the entry
// from pendingChunk Map
func (m *MasterServer) PendingChunkToServerCommit() {
}

func (m *MasterServer) monitorHeartbeats() {
	ticker := time.NewTicker(10 * time.Second)

	for range ticker.C {
		m.mu.Lock()
		for serverID, lastSeen := range m.heartbeats {
			if time.Since(lastSeen) > 15*time.Second {
				log.Printf("chunk server (%s) is DEAD", serverID)
				delete(m.heartbeats, serverID)
				delete(m.chunkMeta, serverID)
			}
		}
		m.mu.Unlock()
	}
}
