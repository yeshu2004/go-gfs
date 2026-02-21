package masterserver

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/yeshu2004/gfs/models"
)


type ChunkMeta struct {
	Addr string
	Disk int64
}

type MasterServer struct {
	listenAddr string
	heartbeats map[string]time.Time
	chunkMeta  map[string]ChunkMeta
	mu         sync.RWMutex
}

// type Master struct {
//     fileToChunks   map[string][]ChunkID
// }

func NewMasterServer(listenAddr string) *MasterServer {
	return &MasterServer{
		listenAddr: listenAddr,
		heartbeats: make(map[string]time.Time),
		chunkMeta:  make(map[string]ChunkMeta),
	}
}

func (m *MasterServer) RunServer() error {
	mux := http.NewServeMux()

	mux.HandleFunc("/register", m.registerChunkServerHandler)
	mux.HandleFunc("/heartbeat", m.heartBeatsHandler)

	go m.monitorHeartbeats()

	log.Printf("master server about to listen on %s", m.listenAddr)
	return http.ListenAndServe(m.listenAddr, mux)
}

func (m *MasterServer) registerChunkServerHandler(rw http.ResponseWriter, req *http.Request){
	if req.Method != http.MethodPost{
		rw.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var pl models.RegisterPayload
	if err := json.NewDecoder(req.Body).Decode(&pl); err != nil{
		rw.WriteHeader(http.StatusBadRequest)
		return
	}

	m.mu.Lock();
	m.chunkMeta[pl.ID] = ChunkMeta{
		Addr: pl.Addr,
		Disk: pl.Disk,
	}
	m.heartbeats[pl.ID] = time.Now();
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
	m.heartbeats[string(pl.ServerID)] = time.Now()
	m.mu.Unlock()

	log.Printf("heartbeat recived - server (%s) is ALIVE with disk space: %d", pl.ServerID, pl.DiskSpace);
	rw.WriteHeader(http.StatusOK)
}

func (m *MasterServer) monitorHeartbeats() {
	ticker := time.NewTicker(10 * time.Second)

	for range ticker.C {
		m.mu.Lock()
		for serverID, lastSeen := range m.heartbeats {
			if time.Since(lastSeen) > 15*time.Second {
				log.Printf("chunk server (%s) is DEAD", serverID)
				delete(m.heartbeats, serverID)
			}
		}
		m.mu.Unlock()
	}
}
