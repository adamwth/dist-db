#!/bin/bash

num_clients=$1

for ((i=1; i<=$num_clients; i++));
do
	echo "Running on ${i}.txt"
	nohup python3 client.py xact-files/${i}.txt >${i}_output.out 2>${i}_stats.out &
done
