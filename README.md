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

In one terminal, run `bin/greeter_server` to get expected output:

```shell
Server listening on 0.0.0.0:50051
```

With server running, in another terminal, run `bin/greeter_client` to get expected output:

```shell
Greeter received: Hello world
```
