#!/bin/bash

# Prepare SSD filesystems
prepare_ssd()
{
  echo "Preparing SSD ..."
  for server in "$MasterAddr" "$ClientAddr" "${ChunkServerAddr[@]}"
  do
    runlocalssh ssh root@"$server" mkfs -t ext4 /dev/elephant/elephant_0_0; rm -rf /export/ssda; mkdir /export/ssda; mount -t ext4 -o noatime,discard,dioread_nolock /dev/elephant/elephant_0_0 /export/ssda
  done
}

MasterPort=50052
ServerPort=11111
BMServerPort=8888

MasterAddr=$2
ClientAddr=$3

gfspath=$1
filespath="$gfspath/files"

# function to prepare gfs
prepare_gfs()
{
  echo "Preparing GFS ..."
  for server in "$MasterAddr" "$ClientAddr" "${ChunkServerAddr[@]}"
  do
    echo "Copying to server: $server"
    runlocalssh ssh root@"$server" mkdir -p $gfspath;
    runlocalssh scp bin/* root@"$server:/$gfspath";
  done
}

if [ "$#" -lt 6 ]; then
  echo "usage: run_gfs.sh path MasterAddr ClientAddr ChunkServerAddr*; Need at least 3 Chunk Servers"
  exit 1
fi
shift 3
ChunkServerAddr=( "$@" )
#prepare_ssd
prepare_gfs

echo "Starting GFS Master ..."
runlocalssh ssh root@"$MasterAddr" -f "$gfspath/gfs_master" "$gfspath/master" "$MasterAddr:$MasterPort" > /tmp/master

echo "Starting Chunk Servers ..."
server_port=$ServerPort
fp=1
for server in "${ChunkServerAddr[@]}"
do
  runlocalssh ssh root@"$server" mkdir -p "$filespath$fp";
  runlocalssh ssh root@"$server" -f "$gfspath/gfs_server" "$MasterAddr:$MasterPort" "$filespath$fp" "$server:$server_port" > "/tmp/server$fp"
  let "server_port += 1"
  let "fp += 1"
done

echo "Starting BM Server ..."
runlocalssh ssh root@"$ClientAddr" -f "$gfspath/bm_server" > /tmp/bmserver

#echo "Starting Client ..."
#runlocalssh ssh root@"$ClientAddr" -f "$gfspath/gfs_client" "$MasterAddr:$MasterPort" "127.0.0.1:8888" -m BENCHMARK > /tmp/client
#echo "Done."
