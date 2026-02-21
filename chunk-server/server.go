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
	if err := c.registerWithMaster(); err != nil {
		log.Fatalln(err)
	}
	c.runHeartBeatCycle();

	mux := http.NewServeMux()

	if err := os.MkdirAll(c.storageDir, os.ModePerm); err != nil {
		log.Println(err.Error())
	}

	log.Printf("chunk server about to listen on %s", c.listenAddr)
	if err := http.ListenAndServe(c.listenAddr, mux); err != nil {
		log.Printf("(%s) server error: %v", c.listenAddr, err)
	}
}

func (c *ChunkServer) runHeartBeatCycle() {
	ticker := time.NewTicker(5 * time.Second)

	for range ticker.C {
		spaceAvail := c.disk - c.used
		if err := sendHeartBeat(c.masterAddr, c.id, spaceAvail); err != nil {
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

func sendHeartBeat(masterServerAddr, serverID string, spaceAvailable int64) error {
	hb := models.HeartBeat{
		ServerID:  models.ServerID(serverID),
		DiskSpace: spaceAvailable,
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
