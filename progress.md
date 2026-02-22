## Progress Update

1) Master Server running at PORT:800
2) Chunk Serve running at PORT: 4001,4002,4003,4004
3) Chunk Server are registered with Master Server before running
4) Heartbeats not recived so declared unactive and removed from heartbeat map
5) Done background Heartbeats at every 5 second 
6) Route GET /chunk-server working i.e. reteriving chunk_id & chunk_servers

## To Implement 
1) Chunk id = logical timestamp when created
2) Master writes an entry to WAL file/ Log file i.e. filename & fileToChunk mapping like Allocate 
923847 Chunk for /logs/filename.txt

