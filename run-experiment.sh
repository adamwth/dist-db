#!/bin/bash

num_clients=$1
num_hosts=$2

for ((i=1; i<=$num_clients; i++));
do
	echo "Running on ${i}.txt, host num $((${i} % ${num_hosts}))"
	nohup python3 client.py xact-files/${i}.txt -hn $((${i} % ${num_hosts})) >${i}_output.out 2>${i}_stats.out &
done
