# CS244B Project

### Team Members
* Wenli Looi (wlooi)
* Ge Bian (gebian)
* Niket Agarwal (niketa)

### How to build and run

This repo contains all dependencies including grpc as git submodules. The final binary is statically linked.

```shell
git clone https://github.com/looi/CS244B.git
cd CS244B
git submodule update --init # clones grpc submodule
cd grpc
git submodule update --init # clones grpc dependencies
make                        # builds grpc
cd third_party/zlib
make                        # builds zlib
cd ../../..                 # back to CS244B root folder
make                        # builds CS244B project
```

#### Run master
In one terminal, run `bin/gfs_master <path to sqlite database>` to get expected output:

```shell
Server listening on 0.0.0.0:50052
```

#### Run chunkserver
In another terminal, run `bin/gfs_server <path to local directory>` to get expected output:

```shell
Server listening on 0.0.0.0:50051
Got server WriteChunk for chunkhandle = 0 and data = new#data0
Got server WriteChunk for chunkhandle = 1 and data = new#data1
Got server WriteChunk for chunkhandle = 2 and data = new#data2
Got server WriteChunk for chunkhandle = 3 and data = new#data3
Got server WriteChunk for chunkhandle = 4 and data = new#data4
Got server WriteChunk for chunkhandle = 5 and data = new#data5
Got server WriteChunk for chunkhandle = 6 and data = new#data6
Got server WriteChunk for chunkhandle = 7 and data = new#data7
Got server WriteChunk for chunkhandle = 8 and data = new#data8
Got server WriteChunk for chunkhandle = 9 and data = new#data9
```

#### Run client
With server running, in another terminal, run `bin/gfs_client` to get expected output:

```shell
Client received: Hello world
Write Chunk written_bytes = 9
Client received chunk data: new#data0
Client received: Hello world
Write Chunk written_bytes = 9
Client received chunk data: new#data1
Client received: Hello world
Write Chunk written_bytes = 9
Client received chunk data: new#data2
Client received: Hello world
Write Chunk written_bytes = 9
Client received chunk data: new#data3
Client received: Hello world
Write Chunk written_bytes = 9
Client received chunk data: new#data4
Client received: Hello world
Write Chunk written_bytes = 9
Client received chunk data: new#data5
Client received: Hello world
Write Chunk written_bytes = 9
Client received chunk data: new#data6
Client received: Hello world
Write Chunk written_bytes = 9
Client received chunk data: new#data7
Client received: Hello world
Write Chunk written_bytes = 9
Client received chunk data: new#data8
Client received: Hello world
Write Chunk written_bytes = 9
Client received chunk data: new#data9
FindLocations file a/aa.txt chunk id 0 got chunkhandle 1
FindLocations file a/ab.txt chunk id 0 got chunkhandle 2
FindLocations file a/aa.txt chunk id 0 got chunkhandle 1
FindLocations file a/aa.txt chunk id 1 got chunkhandle 3
FindLocations file a/b.txt chunk id 0 got chunkhandle 4
FindMatchingFiles filename a/aa.txt
FindMatchingFiles filename a/ab.txt
```

* The local directory should also contain the 10 files with the data the client provided.
* The master's sqlite database should also contain the added files.
