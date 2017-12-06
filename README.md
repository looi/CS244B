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
In one terminal, run `bin/gfs_master <path to sqlite database> master_address (like IP:port)` to get possible output (chunkserver assignment is random):

```shell
Server listening on 127.0.0.1:50052
Found out about new chunkserver: 127.0.0.1:33333
Found out about new chunkserver: 127.0.0.1:44444
Found out about new chunkserver: 127.0.0.1:55555
Found out about new chunkserver: 127.0.0.1:22222
Created new chunkhandle 1
Added new location 127.0.0.1:22222 for chunkhandle 1
Added new location 127.0.0.1:55555 for chunkhandle 1
Added new location 127.0.0.1:44444 for chunkhandle 1
Created new chunkhandle 2
Added new location 127.0.0.1:33333 for chunkhandle 2
Added new location 127.0.0.1:22222 for chunkhandle 2
Added new location 127.0.0.1:44444 for chunkhandle 2
Chunkhandle 2 no longer exists.
DeleteChunks request to 127.0.0.1:22222: (12: Not implemented.)
DeleteChunks request to 127.0.0.1:33333: (12: Not implemented.)
DeleteChunks request to 127.0.0.1:44444: (12: Not implemented.)
```

#### Run chunkserver
In another 3 or more terminals, run `bin/gfs_server master_address (like IP:port) <path_to_local_file_directory> chunkserver_address (like IP:port)` to get possible output from a chunkserver:

```shell
Successfully registered with master.
There's no metadata file for reading: tmp_gfs_33333/metadata127.0.0.1:33333
Heartbeat thread woke up
New chunkhandle hearbeat sent 
Server listening on 127.0.0.1:33333
Got server PushData for clientid = 42 and data = abcde
Got server WriteChunk for chunkhandle = 2
CS location: 127.0.0.1:22222
SerializedWrite bytes_written = 5 at location: 127.0.0.1:22222
CS location: 127.0.0.1:44444
SerializedWrite bytes_written = 5 at location: 127.0.0.1:44444
Heartbeat thread woke up
chunkhandle metadata: 2
New chunkhandle hearbeat sent 
Got DeleteChunks request for 2
```

#### Run client
With server running, in another terminal, run `bin/gfs_client master_address bm_address -m COMMAND` to get possible output:

```shell
Running client in COMMAND mode
Client initialized with id- 42
Usage: <command> <arg1> <arg2> <arg3>...
Options:
	read	<filepath>	<offset>	<length>
	write	<filepath>	<offset>	<data>
	ls	<prefix>
	mv	<filepath>	<new_filepath>
	rm	<filepath>
	quit
> ls
FindMatchingFiles results: 0 files
=======================================
> write test/blah.txt 0 12345
PushData succeeded for data = 12345
PushData succeeded to chunk server 127.0.0.1:22222 for data = 12345
PushData succeeded for data = 12345
PushData succeeded to chunk server 127.0.0.1:55555 for data = 12345
PushData succeeded for data = 12345
PushData succeeded to chunk server 127.0.0.1:44444 for data = 12345
Write Chunk written_bytes = 5
Write status: OK
> write test/abc.txt 0 abcde
PushData succeeded for data = abcde
PushData succeeded to chunk server 127.0.0.1:33333 for data = abcde
PushData succeeded for data = abcde
PushData succeeded to chunk server 127.0.0.1:22222 for data = abcde
PushData succeeded for data = abcde
PushData succeeded to chunk server 127.0.0.1:44444 for data = abcde
Write Chunk written_bytes = 5
Write status: OK
> ls
FindMatchingFiles results: 2 files
=======================================
test/abc.txt
test/blah.txt
> ls test/a
FindMatchingFiles results: 1 files
=======================================
test/abc.txt
> mv test/abc.txt def.txt  
Move status: OK
> ls
FindMatchingFiles results: 2 files
=======================================
def.txt
test/blah.txt
> read def.txt 0 10
Warning: ReadChunk read 5 bytes but asked for 10.
Read status: OK data: abcde     
> rm def.txt
Delete status: OK
> ls
FindMatchingFiles results: 1 files
=======================================
test/blah.txt
```

* The local directory should also contain the 10 files with the data the client provided.
* The master's sqlite database should also contain the added files.
