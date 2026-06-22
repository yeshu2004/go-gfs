package chunkserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/yeshu2004/gfs/models"
)

var (
	MaxUploadSize = 64 * 1024 * 1024
) // 64Bytes

type ChunkServer struct {
	id         string
	listenAddr string
	masterAddr string
	disk       int64
	used       int64
	storageDir string
}

func newRegisterPayload(id, addr string, disk int64) *models.RegisterPayload {
	return &models.RegisterPayload{
		ID:   id,
		Addr: addr,
		Disk: disk,
	}
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
		used:       0,
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

	log.Printf("chunk server about to listen on %s", c.listenAddr)
	if err := http.ListenAndServe(c.listenAddr, mux); err != nil {
		log.Printf("(%s) server error: %v", c.listenAddr, err)
	}
}

func (c *ChunkServer) uploadChunkToServerHandler(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		rw.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// TODO: check sum verification

	chunkID := r.PathValue("chunk_id")
	if chunkID == "" {
		http.Error(rw, "missing chunk id", http.StatusBadRequest)
		return
	}
	log.Println("Requested chunk on Replica Server :", chunkID)

	// check if the chunkId is valid or not, if not return err
	// if yes, then ask the replicas from the primary
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

	log.Printf("Verified ChunkId: %v, Replicas: %v", vaildInfo.ChunkID, vaildInfo.Replicas)

	r.Body = http.MaxBytesReader(rw, r.Body, int64(MaxUploadSize))

	file, fileHeader, err := r.FormFile("video_chunk")
	if err != nil {
		http.Error(rw, "failed to parse form file key 'video'", http.StatusBadRequest)
	}
	defer file.Close()

	buff := make([]byte, 512)
	if _, err := file.Read(buff); err != nil {
		http.Error(rw, "failed to read file headers", http.StatusInternalServerError)
		return
	}

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		http.Error(rw, "Failed to reset file pointer", http.StatusInternalServerError)
		return
	}

	fileType := http.DetectContentType(buff)
	if fileType != "video/mp4" && fileType != "video/webm" && fileType != "application/octet-stream" {
		http.Error(rw, "Invalid file format. Only MP4 and WebM are allowed.", http.StatusBadRequest)
		return
	}

	var ext string

	switch fileType {
	case "video/mp4":
		ext = "mp4"
	case "video/webm":
		ext = "webm"
	case "application/octet-stream":
		ext = "bin" // or infer from original filename
	default:
		http.Error(rw, "Unsupported file type", http.StatusBadRequest)
		return
	}

	fileName := fmt.Sprintf("%s.%s", string(vaildInfo.ChunkID), ext)

	dstPath := filepath.Join(".", "temp", "storage", c.listenAddr, fileName)
	if err := os.MkdirAll(filepath.Dir(dstPath), os.ModePerm); err != nil {
		http.Error(rw, "failed to create storage directory", http.StatusInternalServerError)
		return
	}

	dst, err := os.Create(dstPath)
	if err != nil {
		http.Error(rw, "failed to save file on local machine", http.StatusInternalServerError)
		return
	}

	log.Printf("created file dir path: %s\n", dstPath)
	defer dst.Close()

	if _, err := io.Copy(dst, file); err != nil {
		http.Error(rw, "failed to copy file contents", http.StatusInternalServerError)
		return
	}
	log.Printf("copied file (%v) from client..\n", file)

	// TODO:
	// now replicate it to other replicas
	// and update the master pendingChunk and chunkToServer.... in master

	if err := replicateChunk(dstPath, string(vaildInfo.ChunkID), vaildInfo.Replicas); err != nil{
		// return cleint error maybeee.....
	}

	rw.WriteHeader(http.StatusCreated)
	fmt.Fprintf(rw, "Video successfully uploaded and saved: %s", fileHeader.Filename)
}

func (c *ChunkServer) replicateChunkHandler() {
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


// helper methords
func replicateChunk(dstPath string, chunkID string, replicasAddr []models.ServerID) error {
	var wg sync.WaitGroup

	for _, rserver := range replicasAddr {
		wg.Add(1);
		go func(dstPath string, serverAddr string) {
			defer wg.Done()

			f, err := os.Open(dstPath)
			if err != nil {
				// do error or maybe retry...
			}
			defer f.Close()

			replicaURL := fmt.Sprintf("http://%s/replicate_chunk/%s", serverAddr, chunkID);
			// pass the dstPath in header
			resp, err := http.Post(replicaURL, "application/octet-stream", f)
			if err != nil {
				// retryy.....
				f, err = os.Open(dstPath);
				for attempt :=0; attempt < 3; attempt++{
					// retry noww....
				}
			}


			if resp.StatusCode != http.StatusOK{
				// retry & dont decs the wg....
			}

			defer resp.Body.Close()

		}(dstPath, string(rserver))
	}

	wg.Wait()
	return nil;
}

func sendHeartBeat(masterServerAddr, serverID string, diskSpace, diskUsed int64) error {
	// uncomment below for testing dead server validation
	// if serverID == "chunk-server-1"{
	// 	return nil;
	// }
	hb := models.HeartBeat{
		ServerID:       models.ServerID(serverID),
		TotalDiskSpace: diskSpace,
		DiskUsed:       diskUsed,
	}
	pl, err := json.Marshal(hb)
	if err != nil {
		return err
	}

	client := http.Client{
		Timeout: 2 * time.Second,
	}

	url := fmt.Sprintf("http://%s/heartbeat", masterServerAddr)
	_, err = client.Post(url, "application/json", bytes.NewBuffer(pl))
	return err
}
