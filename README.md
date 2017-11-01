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

In one terminal, run `bin/gfs_server <path to local directory>` to get expected output:

```shell
Server listening on 0.0.0.0:50051
```

With server running, in another terminal, run `bin/gfs_client` to get expected output:

```shell
Client received: Hello world
Write Chunk returned: 0
Client received chunk data: new#data0
Client received: Hello world
Write Chunk returned: 0
Client received chunk data: new#data1
Client received: Hello world
Write Chunk returned: 0
Client received chunk data: new#data2
Client received: Hello world
Write Chunk returned: 0
Client received chunk data: new#data3
Client received: Hello world
Write Chunk returned: 0
Client received chunk data: new#data4
Client received: Hello world
Write Chunk returned: 0
Client received chunk data: new#data5
Client received: Hello world
Write Chunk returned: 0
Client received chunk data: new#data6
Client received: Hello world
Write Chunk returned: 0
Client received chunk data: new#data7
Client received: Hello world
Write Chunk returned: 0
Client received chunk data: new#data8
Client received: Hello world
Write Chunk returned: 0
Client received chunk data: new#data9
```

The local directory should also contain the 10 files with the data the client provided.
