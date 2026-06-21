package chunkserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
	"github.com/yeshu2004/gfs/models"
)

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

	chunkID := r.PathValue("chunk_id")
	if chunkID == "" {
		http.Error(rw, "missing chunk id", http.StatusBadRequest)
		return
	}
	log.Println("Requested chunk on Replica Server :", chunkID)

	// check if the chunkId is valid or not, if not return err
	// if yes, then ask the replicas from the primary
	masterNodeUrl := fmt.Sprintf("http://%s/chunk-info/%s", c.masterAddr, chunkID)
	resp, err := http.Get(masterNodeUrl);
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		http.Error(rw, "master returned error", resp.StatusCode)
		return
	}

	var vaildInfo models.VerfiyChunkResp;
	if err := json.NewDecoder(resp.Body).Decode(&vaildInfo); err != nil{
		http.Error(rw, "chunk decoding error", http.StatusInternalServerError);
		return
	};


	log.Printf("ChunkId: %v, Replicas: %v", vaildInfo.ChunkID, vaildInfo.Replicas);
	rw.WriteHeader(http.StatusOK)
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
