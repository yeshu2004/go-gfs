package chunkserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yeshu2004/gfs/models"
)

var (
	ChunkSize     = 64 * 1024 * 1024 // 64 MB (used by master/frontend)
    MaxUploadSize = 65 * 1024 * 1024 // 65 MB (server accepts multipart overhead)
)

const (
	maxRetries       = 3
	retryDelay       = 500 * time.Millisecond
	replicateTimeout = 30 * time.Second
)

type ChunkServer struct {
	id         string
	listenAddr string
	masterAddr string
	disk       int64
	used       atomic.Int64
	storageDir string
}

func newRegisterPayload(id, addr string, disk int64) *models.RegisterPayload {
	return &models.RegisterPayload{
		ID:   id,
		Addr: addr,
		Disk: disk,
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

func NewChunkServer(serverID string, listenAddr string, masterAddr string, disksize int64) *ChunkServer {
	port := strings.TrimPrefix(listenAddr, ":")
	dir := fmt.Sprintf("temp/storage/%s", port)
	log.Println(dir)

	return &ChunkServer{
		id:         serverID,
		listenAddr: listenAddr,
		disk:       disksize,
		masterAddr: masterAddr,
		used:       atomic.Int64{},
		storageDir: dir,
	}
}

func (c *ChunkServer) RunServer() {
	if err := os.MkdirAll(c.storageDir, os.ModePerm); err != nil {
		log.Println(err.Error())
	}

	if err := c.registerWithMaster(); err != nil {
		log.Fatalln(err)
	}
	go c.runHeartBeatCycle()

	mux := http.NewServeMux()
	mux.HandleFunc("/upload/{chunk_id}", c.uploadChunkToServerHandler)
	mux.HandleFunc("/replicate_chunk/{chunk_id}", c.replicateChunkHandler)

	handler := corsMiddleware(mux)

	log.Printf("chunk server about to listen on %s", c.listenAddr)
	if err := http.ListenAndServe(c.listenAddr, handler); err != nil {
		log.Printf("(%s) server error: %v", c.listenAddr, err)
	}
}

// --------------- HTTP HANDLER ---------------------

func (c *ChunkServer) uploadChunkToServerHandler(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		rw.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	chunkID := r.PathValue("chunk_id")
	if chunkID == "" {
		http.Error(rw, "missing chunk id", http.StatusBadRequest)
		return
	}
	log.Println("Requested chunk on Replica Server:", chunkID)

	masterNodeUrl := fmt.Sprintf("http://%s/chunk-info/%s", c.masterAddr, chunkID)
	resp, err := http.Get(masterNodeUrl)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		http.Error(rw, "master returned error", resp.StatusCode)
		return
	}

	var vaildInfo models.VerfiyChunkResp
	if err := json.NewDecoder(resp.Body).Decode(&vaildInfo); err != nil {
		http.Error(rw, "chunk decoding error", http.StatusInternalServerError)
		return
	}

	log.Printf("Verified ChunkId: %v, Replicas: %v", vaildInfo.ChunkID, vaildInfo.ReplicaAddrs)

	r.Body = http.MaxBytesReader(rw, r.Body, int64(MaxUploadSize))

	file, fileHeader, err := r.FormFile("video_chunk")
	if err != nil {
		http.Error(rw, fmt.Sprintf("failed to parse form file key 'video': %v", err), http.StatusBadRequest)
		return
	}
	defer file.Close()

	// buff := make([]byte, 512)
	// if _, err := file.Read(buff); err != nil {
	// 	http.Error(rw, "failed to read file headers", http.StatusInternalServerError)
	// 	return
	// }

	// if _, err := file.Seek(0, io.SeekStart); err != nil {
	// 	http.Error(rw, "Failed to reset file pointer", http.StatusInternalServerError)
	// 	return
	// }

	// fileType := http.DetectContentType(buff)
	// if fileType != "video/mp4" && fileType != "video/webm" && fileType != "application/octet-stream" {
	// 	http.Error(rw, "Invalid file format. Only MP4 and WebM are allowed.", http.StatusBadRequest)
	// 	return
	// }

	// var ext string
	// switch fileType {
	// case "video/mp4":
	// 	ext = "mp4"
	// case "video/webm":
	// 	ext = "webm"
	// case "application/octet-stream":
	// 	ext = "bin"
	// default:
	// 	http.Error(rw, "Unsupported file type", http.StatusBadRequest)
	// 	return
	// }

	fileName := fmt.Sprintf("%s", string(vaildInfo.ChunkID))
	dstPath := filepath.Join(c.storageDir, fileName)
	if err := os.MkdirAll(filepath.Dir(dstPath), os.ModePerm); err != nil {
		http.Error(rw, "failed to create storage directory", http.StatusInternalServerError)
		return
	}

	dst, err := os.Create(dstPath)
	if err != nil {
		http.Error(rw, "failed to save file on local machine", http.StatusInternalServerError)
		return
	}
	defer dst.Close()

	if _, err := io.Copy(dst, file); err != nil {
		http.Error(rw, "failed to copy file contents", http.StatusInternalServerError)
		return
	}

	fileInfo , err := os.Stat(dstPath);
	if err != nil{
		http.Error(rw, "failed to get describing the named file.", http.StatusInternalServerError)
		return
	}
	fileSize := fileInfo.Size();
	c.used.Add(fileSize);

	log.Printf("copied file (%v), size (%d) from client\n", fileHeader.Filename, fileSize);

	if err := replicateChunk(dstPath, string(vaildInfo.ChunkID), vaildInfo.ReplicaAddrs); err != nil {
		log.Printf("replication chunk error: %v\n", err)
		// NOTE: chunk is saved locally but replication failed.
		// Returning 500 here lets the client know to retry or alert.
		http.Error(rw, "chunk saved but replication failed", http.StatusInternalServerError)
		return
	}

	// update the master state.....
	res, err := http.Post(fmt.Sprintf("http://%s/update_file_metadata/%s", c.masterAddr, chunkID),"application/json; charset=utf-8", nil);

	if err != nil{
		http.Error(rw, fmt.Sprintf("error in updating file chunk metadata: %v", err), http.StatusInternalServerError);
		return;
	}

	if res.StatusCode != http.StatusOK{
		http.Error(rw, "status code error in updating file chunk metadata", http.StatusInternalServerError);
		return;
	}

	rw.WriteHeader(http.StatusCreated)
	fmt.Fprintf(rw, "Video successfully uploaded and saved: %s", fileHeader.Filename)
}


func (c *ChunkServer) replicateChunkHandler(rw http.ResponseWriter, r *http.Request) {
	chunkID := r.PathValue("chunk_id")
	if chunkID == "" {
		http.Error(rw, "missing chunk ID", http.StatusBadRequest)
		return
	}

	dstPath := filepath.Join(c.storageDir, chunkID)
	f, err := os.Create(dstPath)

	if err != nil {
		log.Printf("replicateChunkHandler: failed to create file %s: %v", dstPath, err)
		http.Error(rw, "failed to create chunk file", http.StatusInternalServerError)
		return
	}
	defer f.Close()

	if _, err := io.Copy(f, r.Body); err != nil {
		log.Printf("replicateChunkHandler: failed to write chunk %s: %v", chunkID, err)
		os.Remove(dstPath) // clean up partial file
		http.Error(rw, "failed to write chunk", http.StatusInternalServerError)
		return
	}

	fileInfo, err := f.Stat();
	if err != nil{
		http.Error(rw, "failed to get describing the named file.", http.StatusInternalServerError)
		return
	}
	fileSize := fileInfo.Size();
	c.used.Add(fileSize);
	log.Printf("%s - disk size updated....", c.listenAddr);

	log.Printf("(%s) - copied the file: %s on the server (%s)", r.RemoteAddr, chunkID, c.listenAddr);
	rw.WriteHeader(http.StatusOK)
}


// ---------------- HELPER FUNCTION -----------------------------


// replicateChunk fans out the chunk at dstPath to all replica servers in parallel.
// it returns a joined error if any replica fails after all retries.
func replicateChunk(dstPath string, chunkID string, replicasAddr map[models.ServerID]string) error {
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)

	for _, rserver := range replicasAddr {
		wg.Add(1)
		go func(serverAddr string) {
			defer wg.Done()
			if err := sendChunkWithRetry(dstPath, chunkID, serverAddr); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("replica %s: %w", serverAddr, err))
				mu.Unlock()
			}
		}(string(rserver))
	}

	wg.Wait()
	return errors.Join(errs...)
}

// sendChunkWithRetry attempts to POST the chunk to serverAddr with linear back-off.
func sendChunkWithRetry(dstPath, chunkID, serverAddr string) error {
	replicaURL := fmt.Sprintf("http://%s/replicate_chunk/%s", serverAddr, chunkID)

	var lastErr error
	for attempt := range maxRetries {
		if attempt > 0 {
			time.Sleep(retryDelay * time.Duration(attempt))
		}
		if err := sendChunk(dstPath, replicaURL); err != nil {
			lastErr = err
			log.Printf("sendChunkWithRetry: attempt %d/%d failed for %s: %v", attempt+1, maxRetries, serverAddr, err)
			continue
		}
		return nil
	}

	return fmt.Errorf("all %d attempts failed: %w", maxRetries, lastErr)
}

// sendChunk performs a single POST of the chunk file to replicaURL.
func sendChunk(dstPath, replicaURL string) error {
	f, err := os.Open(dstPath)
	if err != nil {
		return fmt.Errorf("open chunk file: %w", err)
	}
	defer f.Close()

	ctx, cancel := context.WithTimeout(context.Background(), replicateTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, replicaURL, f)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("http post: %w", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	log.Printf("successfully send chunk to (%s) over the newtwork\n", replicaURL)
	return nil
}

func (c *ChunkServer) runHeartBeatCycle() {
	ticker := time.NewTicker(5 * time.Second)
	for range ticker.C {
		if err := sendHeartBeat(c.masterAddr, c.id, c.disk, c.used); err != nil {
			log.Printf("(%s) server failed to send heartbeat, err: %v\n", c.id, err)
		}
	}
}

func (c *ChunkServer) registerWithMaster() error {
	payload := newRegisterPayload(c.id, c.listenAddr, c.disk)

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("http://%s/register", c.masterAddr)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("(%s): error in reading the register response: %v", c.id, err)
		}
		return fmt.Errorf("(%s): registration failed: %s", c.id, string(b))
	}

	log.Printf("(%s): chunk server registered with master", c.id)
	return nil
}

func sendHeartBeat(masterServerAddr, serverID string, diskSpace int64, diskUsed atomic.Int64) error {
	hb := models.HeartBeat{
		ServerID:       models.ServerID(serverID),
		TotalDiskSpace: diskSpace,
		DiskUsed:       diskUsed.Load(),
	}
	pl, err := json.Marshal(hb)
	if err != nil {
		return err
	}

	client := http.Client{Timeout: 2 * time.Second}
	url := fmt.Sprintf("http://%s/heartbeat", masterServerAddr)
	_, err = client.Post(url, "application/json", bytes.NewBuffer(pl))
	return err
}