package main

import (
	"fmt"
	"log"

	cs "github.com/yeshu2004/gfs/chunk-server"
	ms "github.com/yeshu2004/gfs/master-server"
)

func main() {
	masterListenAddr := ":8000"
	msr := ms.NewMasterServer(masterListenAddr)

	go func() {
		if err := msr.RunServer(); err != nil {
			log.Fatalln(err)
		}
	}()

	chunkServerAddr := []string{":4001", ":4002", ":4003", ":4004"}
	var disksize int64 = 1_000_000_000
	for i, addr := range chunkServerAddr{
		go func (addr string)  {
			chunkServerID := fmt.Sprintf("CS%d", i+1)
			csr := cs.NewChunkServer(chunkServerID, addr, masterListenAddr, disksize);
			csr.RunServer();
		}(addr);
	}

	select{}
}
