#!/bin/bash

num_clients=$1

for ((i=1; i<=$num_clients; i++));
do
	echo "Running on ${i}.txt, host num $(((${i} % 5) + 1))"
	nohup python3 client.py xact-files/${i}.txt -hn $(((${i} % 5) + 1)) >${i}_output.out 2>${i}_stats.out &
done
