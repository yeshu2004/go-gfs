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

type PendingChunk struct {
	ChunkID  models.ChunkID
	Primary  models.ServerID
	Replicas []models.ServerID
	// Checksum string
	// Size     int64
}

// Q) Do we have to store the meta data in the master server i.e for the file we would store the
// file meta data like filsize, file uuid genrated, file to chunk mapping(already implemented), also
// we can store the checksum to on each chunk meta data ?

type MasterServer struct {
	listenAddr string
	heartbeats map[models.ServerID]time.Time
	chunkMeta  map[models.ServerID]ChunkMeta
	mu         sync.RWMutex

	fileMetaData map[string]models.FileMetaData
	// fileToChunks  map[string][]models.ChunkID
	pendingChunks map[models.ChunkID]PendingChunk // temprory state
	chunkToServer map[models.ChunkID]PendingChunk // update: after commit to disk
}

func NewMasterServer(listenAddr string) *MasterServer {
	return &MasterServer{
		listenAddr:   listenAddr,
		heartbeats:   make(map[models.ServerID]time.Time),
		chunkMeta:    make(map[models.ServerID]ChunkMeta),
		fileMetaData: make(map[string]models.FileMetaData),
		// fileToChunks:  make(map[string][]models.ChunkID),
		pendingChunks: make(map[models.ChunkID]PendingChunk),
		chunkToServer: make(map[models.ChunkID]PendingChunk),
	}
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (m *MasterServer) RunServer() error {
	mux := http.NewServeMux()

	mux.HandleFunc("/register", m.registerChunkServerHandler) // WORKING
	mux.HandleFunc("/heartbeat", m.heartBeatsHandler)         // WORKING
	mux.HandleFunc("/chunk-server", m.allocateChunkHandler)   // WORKING
	mux.HandleFunc("/max-chunk-size", m.returnMaxChunkSizeHandler)
	mux.HandleFunc("/chunk-info/{chunk_id}", m.verfiyAndChunkInfoHandler) // WORKING
	mux.HandleFunc("/update_file_metadata/{chunk_id}", m.updateFileMetaData)
	mux.HandleFunc("/file-info/{file_id}", m.fileInfoHandler) // WORKING

	go m.monitorHeartbeats()

	handler := corsMiddleware(mux)

	log.Printf("master server about to listen on %s", m.listenAddr)
	return http.ListenAndServe(m.listenAddr, handler)
}

func (m *MasterServer) fileInfoHandler(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(rw, fmt.Sprintf("%v not allowed", r.Method), http.StatusMethodNotAllowed)
		return
	}

	fileID := r.PathValue("file_id")
	if fileID == "" {
		http.Error(rw, "missing file id", http.StatusBadRequest)
		return
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	fileMeta, ok := m.fileMetaData[fileID]
	if !ok {
		http.Error(rw, fmt.Sprintf("file (%s) not found", fileID), http.StatusNotFound)
		return
	}

	chunks := make([]models.ChunkLocation, 0, len(fileMeta.ChunkIDs))
	for _, chunkID := range fileMeta.ChunkIDs {
		committed, ok := m.chunkToServer[chunkID]
		if !ok {
			http.Error(rw, fmt.Sprintf("file (%s) is not fully committed, chunk (%s) missing", fileID, chunkID), http.StatusConflict)
			return
		}

		primaryAddr := m.chunkMeta[committed.Primary].Addr
		chunks = append(chunks, models.ChunkLocation{
			ChunkID:     committed.ChunkID,
			Primary:     committed.Primary,
			PrimaryAddr: primaryAddr,
			Replicas:    committed.Replicas,
		})
	}

	rw.Header().Set("Content-Type", "application/json")
	json.NewEncoder(rw).Encode(models.FileInfoResponse{
		FileID:    fileMeta.FileID,
		FileName:  fileMeta.FileName,
		FileType:  fileMeta.FileType,
		Size:      fileMeta.Size,
		CreatedAt: fileMeta.CreatedAt,
		Chunks:    chunks,
	})
}

// basically delete pending chunk state in its data
func (m *MasterServer) updateFileMetaData(rw http.ResponseWriter, r *http.Request) {
	chunkID := r.PathValue("chunk_id")

	if chunkID == "" {
		http.Error(rw, "missing chunk id", http.StatusBadRequest)
		return
	}
	log.Println("requested chunk on Maaster Server for updating file MetaData :", chunkID)

	m.mu.Lock()
	defer m.mu.Unlock()

	pending := m.pendingChunks[models.ChunkID(chunkID)]
	delete(m.pendingChunks, models.ChunkID(chunkID))
	log.Printf("pending chunk (%s) state updated i.e deleted", chunkID)

	_, exists := m.chunkToServer[models.ChunkID(chunkID)]
	if exists {
		http.Error(rw, "chunkID already commited in server !! (should never happen)", http.StatusBadRequest)
		return
	}
	m.chunkToServer[models.ChunkID(chunkID)] = pending

	// TODO: update the file meta data i.e status = "commited" if successfull

	log.Printf("chunk (%s) state updated...", chunkID)
	rw.WriteHeader(http.StatusOK)
}


// TODO: after registering the file meta data and returing the chunk size allowed by client over 
// network, if the user doesn't even upload the file then, we have to actully delete those 
// file meta data information as they would not be upload, hence a TTL logic would be a better 
// solution to apply with a 5*60 sec ttl.
func (m *MasterServer) returnMaxChunkSizeHandler(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(rw, fmt.Sprintf("%v not allowed", r.Method), http.StatusMethodNotAllowed)
		return
	}

	// genrate a uuid i.e fileID
	type metaData struct {
		FileName string `json:"file_name"`
		Size     int64  `json:"size"`
		FileType string `json:"file_type"`
	}

	var fileMetaInfo metaData
	if err := json.NewDecoder(r.Body).Decode(&fileMetaInfo); err != nil {
		http.Error(rw, "decoder error at fileMetaData", http.StatusBadRequest)
		return
	}

	fileID := uuid.NewString()
	fileInfo := models.FileMetaData{
		FileID:    fileID,
		FileName:  fileMetaInfo.FileName,
		FileType:  fileMetaInfo.FileType,
		Size:      fileMetaInfo.Size,
		CreatedAt: time.Now(),
		Status:    "pending",
	}

	m.mu.Lock()
	m.fileMetaData[fileID] = fileInfo
	m.mu.Unlock()

	type ChunkSizeReponse struct {
		FileID       string    `json:"file_id"`
		FileName     string    `json:"file_name"`
		FileType     string    `json:"file_type"`
		Size         int64     `json:"size"`
		CreatedAt    time.Time `json:"created_at"`
		Status       string    `json:"status"`
		MaxChunkSize int64     `json:"max_chunk_size"`
	}

	rw.Header().Set("Content-Type", "application/json")
	json.NewEncoder(rw).Encode(ChunkSizeReponse{
		FileID:       fileInfo.FileID,
		FileName:     fileInfo.FileName,
		FileType:     fileInfo.FileType,
		Size:         fileInfo.Size,
		CreatedAt:    fileInfo.CreatedAt,
		Status:       fileInfo.Status,
		MaxChunkSize: ChunkSize,
	})
}

func (m *MasterServer) verfiyAndChunkInfoHandler(rw http.ResponseWriter, r *http.Request) {
	chunkID := r.PathValue("chunk_id")
	if chunkID == "" {
		http.Error(rw, "missing chunk id", http.StatusBadRequest)
		return
	}
	log.Println("Requested chunk on Maaster Server :", chunkID)

	chunk, ok := m.pendingChunks[models.ChunkID(chunkID)]
	log.Printf("Exists? %v", ok)
	if !ok {
		http.Error(rw, "chunk not found", http.StatusNotFound)
		return
	}

	if len(chunk.Replicas) == 0 {
		http.Error(rw, fmt.Sprintf("%s doesn't have any replicas or is not registered with us...", chunkID), http.StatusBadRequest)
		return
	}

	m.mu.RLock()
	replicaAddrs := make(map[models.ServerID]string, len(chunk.Replicas))
	for _, id := range chunk.Replicas {
		if meta, ok := m.chunkMeta[id]; ok {
			replicaAddrs[id] = meta.Addr
		} else {
			log.Printf("verfiyAndChunkInfoHandler: no addr found for replica %s", id)
		}
	}
	m.mu.RUnlock()

	rw.Header().Set("Content-Type", "application/json")
	json.NewEncoder(rw).Encode(models.VerfiyChunkResp{
		ChunkID:      chunk.ChunkID,
		Replicas:     chunk.Replicas,
		ReplicaAddrs: replicaAddrs,
	})
}


func (m *MasterServer) allocateChunkHandler(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(rw, fmt.Sprintf("%v not allowed", r.Method), http.StatusMethodNotAllowed)
		return
	}
	log.Printf("%s methord recived at %s", r.Method, r.URL.Path)

	// 0. recive filename along with every chunk
	type reqBody struct {
		FileID string `json:"file_id"`
	}

	var pl reqBody
	if err := json.NewDecoder(r.Body).Decode(&pl); err != nil {
		http.Error(rw, "invalid request", http.StatusBadRequest)
		return
	}

	log.Printf("fileID: %s", pl.FileID)

	m.mu.Lock()
	defer m.mu.Unlock()

	// 1. ensure file entry exists
	meta, ok := m.fileMetaData[pl.FileID]
	if !ok {
		http.Error(rw, fmt.Sprintf("file (%s) not found", pl.FileID), http.StatusNotFound)
		return
	}
	log.Printf("fileSize: %s, fileName: %s", meta.FileID, meta.FileName)

	// DONE:
	// if user has a file to upload i.e it would have multiple chunks which would have to be uploded
	// so each file should have a unique name corrosponding to it, and to make it unique every time either
	// we could version it if user upload the same file again and again or we have gentrate a new uuid or
	// we would not allow the same file name to be in the our storage server. It's basiclly how we design
	// and what we want from the user.

	// to make every file unqiue we could genrate a uuid (128 bit) and assign every file a uuid i.e. only
	// uuid will be saved not file name ,so if a user upload the same file again, it would genrate a new
	// uuid back again making it a new unique file again. Then we would have to map each file UUID with the
	// chunk UUID'S.

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
		http.Error(rw, fmt.Sprintf("not enough servers with space, as replication factor: Elibigle Server (%d) - Replcation Factor (%d)\n", len(eligibleServers), ReplicationFactor), http.StatusServiceUnavailable)
		return
	}

	// 3. shuffle among the eligible servers
	rand.Shuffle(len(eligibleServers), func(i, j int) {
		eligibleServers[i], eligibleServers[j] = eligibleServers[j], eligibleServers[i]
	})
	selectedServer := eligibleServers[:ReplicationFactor] // quoram gaurentee

	// 4. genrate chunkID
	// have to look for better small id bcz uuid are 128 bit,
	// paper suggests for 64 bit bcz for memory efficiency
	chunkId := uuid.New().String()

	// 5. substract the available space for those server for now
	for _, server := range selectedServer {
		meta := m.chunkMeta[server]
		meta.ReservedDisk += ChunkSize
		m.chunkMeta[server] = meta

	}

	// 6. update fileToChunks & chunkToServer
	// m.fileToChunks[pl.FileName] = append(m.fileToChunks[pl.FileName], models.ChunkID(chunkId))
	meta.ChunkIDs = append(meta.ChunkIDs, models.ChunkID(chunkId))
	m.fileMetaData[pl.FileID] = meta

	m.pendingChunks[models.ChunkID(chunkId)] = PendingChunk{
		ChunkID:  models.ChunkID(chunkId),
		Primary:  selectedServer[0],
		Replicas: selectedServer[1:],
	}

	log.Printf("Added pending chunk: %s\n", chunkId)
	log.Printf("Current pending map: %+v\n", m.pendingChunks)

	type AllocateChunkResponse struct {
		ChunkID     models.ChunkID    `json:"chunk_id"`
		Primary     models.ServerID   `json:"primary_server_name"`
		Replicas    []models.ServerID `json:"replica_servers"`
		PrimaryAddr string            `json:"primary_addr"`
		ChunkSize   int64             `json:"chunk_size"`
	}

	primaryServer := selectedServer[0]
	primaryAddr := m.chunkMeta[primaryServer].Addr

	// 7. retrun back the chunkID & servers to client
	rw.Header().Set("Content-Type", "application/json")
	json.NewEncoder(rw).Encode(AllocateChunkResponse{
		ChunkID:     models.ChunkID(chunkId),
		Primary:     primaryServer,
		Replicas:    selectedServer[1:],
		PrimaryAddr: primaryAddr,
		ChunkSize:   ChunkSize,
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
