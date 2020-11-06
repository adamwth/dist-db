#!/bin/bash

node1=$1
node2=$2
node3=$3
node4=$4
node5=$5
port=$6
listen_host=$7

./cockroach start --insecure --cache=.25 --max-sql-memory=.25 --store=dbproject --listen-addr=xcnc${listen_host}.comp.nus.edu.sg:${port} --http-addr=xcnc${listen_host}.comp.nus.edu.sg:8009 --join=xcnc${node1}.comp.nus.edu.sg:${port},xcnc${node2}.comp.nus.edu.sg:${port},xcnc${node3}.comp.nus.edu.sg:${port},xcnc${node4}.comp.nus.edu.sg:${port},xcnc${node5}.comp.nus.edu.sg:${port} --background
