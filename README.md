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
Currently the chunkserver addressress are hardcoded to 127.0.0.1:33333,
127.0.0.1:44444, 127.0.0.1:55555
In another 3 terminals, run `bin/gfs_server master_address (like IP:port) <path_to_local_file_directory> chunkserver_address (like IP:port)` to get expected output from 3 chunkservers:

```shell
Server listening on 127.0.0.1:33333
Got server PushData for clientid = 42 and data = new#data0
Got server WriteChunk for chunkhandle = 1
CS location: 127.0.0.1:44444
SerializedWrite bytes_written = 9 at location: 127.0.0.1:44444
CS location: 127.0.0.1:55555
SerializedWrite bytes_written = 9 at location: 127.0.0.1:55555
New chunkhandle hearbeat sent for: 0
Got server PushData for clientid = 42 and data = new#data1
Got server WriteChunk for chunkhandle = 2
CS location: 127.0.0.1:44444
SerializedWrite bytes_written = 9 at location: 127.0.0.1:44444
CS location: 127.0.0.1:55555
SerializedWrite bytes_written = 9 at location: 127.0.0.1:55555
New chunkhandle hearbeat sent for: 1
```

```shell
Server listening on 127.0.0.1:44444
Got server PushData for clientid = 42 and data = new#data0
New chunkhandle hearbeat sent for: 0
Got server PushData for clientid = 42 and data = new#data1
New chunkhandle hearbeat sent for: 1
```

```shell
Server listening on 127.0.0.1:55555
Got server PushData for clientid = 42 and data = new#data0
New chunkhandle hearbeat sent for: 0
Got server PushData for clientid = 42 and data = new#data1
New chunkhandle hearbeat sent for: 1
```

#### Run client
With server running, in another terminal, run `bin/gfs_client` to get expected output:

```shell
FindMatchingFiles results: 0 files
=======================================
Read status: (5: File does not exist.) data: 
PushData succeeded for data = new#data0
PushData succeeded to chunk server 127.0.0.1:33333 for data = new#data0
PushData succeeded for data = new#data0
PushData succeeded to chunk server 127.0.0.1:44444 for data = new#data0
PushData succeeded for data = new#data0
PushData succeeded to chunk server 127.0.0.1:55555 for data = new#data0
Write Chunk written_bytes = 9
Write status: OK
Read status: OK data: new#data0
File a/test0.txt num_chunks = 1
PushData succeeded for data = new#data1
PushData succeeded to chunk server 127.0.0.1:33333 for data = new#data1
PushData succeeded for data = new#data1
PushData succeeded to chunk server 127.0.0.1:44444 for data = new#data1
PushData succeeded for data = new#data1
PushData succeeded to chunk server 127.0.0.1:55555 for data = new#data1
Write Chunk written_bytes = 9
Write status: OK
Read status: OK data: new#data1
File a/test1.txt num_chunks = 1
FindMatchingFiles results: 2 files
=======================================
a/test0.txt
a/test1.txt
```

* The local directory should also contain the 10 files with the data the client provided.
* The master's sqlite database should also contain the added files.
