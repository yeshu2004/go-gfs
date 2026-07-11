package main

import (
	"fmt"
	"log"

	cs "github.com/yeshu2004/gfs/chunk-server"
	ms "github.com/yeshu2004/gfs/master-server"
)

var (
	MasterServerAddr = ":8000"
	ChunkServerAddr = []string{":4001", ":4002", ":4003", ":4004"}
	ServerDiskSpace = 10_000_000_000 // 10 GB per server...
)


func newGfsSever(masterNodeAddr string,  chunkServersAddr []string, diskSpace int64){
	msn := ms.NewMasterServer(masterNodeAddr);
	go func() {
		if err := msn.RunServer(); err != nil {
			log.Fatalln(err)
		}
	}()


	for i, addr := range chunkServersAddr{
		go func (addr string)  {
			chunkServerID := fmt.Sprintf("CS%d", i+1)
			csr := cs.NewChunkServer(chunkServerID, addr, masterNodeAddr, diskSpace);
			csr.RunServer();
		}(addr);
	}
}

func main() {
	newGfsSever(MasterServerAddr, ChunkServerAddr, int64(ServerDiskSpace));
	select{};	
}
