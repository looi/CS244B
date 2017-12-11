#!/bin/bash
# Screen cs244demo needs to be a *new* screen running already.
# In a separate terminal, run screen -S cs244demo

rm -rf tmp_gfs*

screen -S cs244demo -X split -v
screen -S cs244demo -X split
screen -S cs244demo -X stuff "bin/gfs_master tmp_gfs_master.sqlite3 127.0.0.1:50052^M"
screen -S cs244demo -X focus right
screen -S cs244demo -X split
screen -S cs244demo -X split
screen -S cs244demo -X split

mkdir -p tmp_gfs_33333
screen -S cs244demo -X screen
screen -S cs244demo -X stuff "bin/gfs_server 127.0.0.1:50052 tmp_gfs_33333 127.0.0.1:33333^M"
screen -S cs244demo -X focus next

mkdir -p tmp_gfs_33334
screen -S cs244demo -X screen
screen -S cs244demo -X stuff "bin/gfs_server 127.0.0.1:50052 tmp_gfs_33334 127.0.0.1:33334^M"
screen -S cs244demo -X focus next

mkdir -p tmp_gfs_33335
screen -S cs244demo -X screen
screen -S cs244demo -X stuff "bin/gfs_server 127.0.0.1:50052 tmp_gfs_33335 127.0.0.1:33335^M"
screen -S cs244demo -X focus next

mkdir -p tmp_gfs_33336
screen -S cs244demo -X screen
screen -S cs244demo -X stuff "bin/gfs_server 127.0.0.1:50052 tmp_gfs_33336 127.0.0.1:33336^M"
screen -S cs244demo -X focus next

screen -S cs244demo -X focus left
screen -S cs244demo -X focus next
screen -S cs244demo -X screen
screen -S cs244demo -X stuff "bin/gfs_client 127.0.0.1:50052 127.0.0.1:8888 -m COMMAND^M"
