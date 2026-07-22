package main

import (
	"fmt"
	"log"
	"time"

	cs "github.com/yeshu2004/gfs/chunk-server"
	ms "github.com/yeshu2004/gfs/master-server"
	"github.com/yeshu2004/gfs/wal"
)

var (
	MasterServerAddr = ":8000"
	ChunkServerAddr  = []string{":4001", ":4002", ":4003", ":4004"}
	ServerDiskSpace  = 10_000_000_000 // 10 GB per server...
)

func newGfsSever(masterNodeAddr string, chunkServersAddr []string, diskSpace int64, walDir string) {
	w, err := wal.OpenWAL(walDir, false)
	if err != nil {
		log.Fatalf("OpenWAL Error: %v\n", err)
	}

	time.Sleep(1 * time.Second) // otherwise connection refuse...
	msn := ms.NewMasterServer(masterNodeAddr, w)
	go func() {
		if err := msn.RunServer(); err != nil {
			log.Fatalln(err)
		}
	}()

	for i, addr := range chunkServersAddr {
		go func(i int, addr string) {
			chunkServerID := fmt.Sprintf("CS%d", i+1)
			csr := cs.NewChunkServer(chunkServerID, addr, masterNodeAddr, diskSpace)
			csr.RunServer()
		}(i, addr)
	}
}

func main() {
	newGfsSever(MasterServerAddr, ChunkServerAddr, int64(ServerDiskSpace), "./bin/wal")
	select {}
}
