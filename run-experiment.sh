#!/bin/bash

num_clients=$1

declare -a hosts
declare -a ports
####### CONFIGURATION OF MACHINES ##############
hosts[0]=0
ports[0]=26260
hosts[1]=1
ports[1]=26260
hosts[2]=2
ports[2]=26260
hosts[3]=3
ports[3]=26260
hosts[4]=4
ports[4]=26260
################################################

if [[ "${#hosts[@]}" -ne "${#ports[@]}" ]] ; then
  echo "Configuration hosts and ports do not match up"
  exit 1
fi

num_hosts=${#hosts[@]}

for ((i=0; i<$num_clients; i++));
do
	echo "Running on $((${i} + 1)).txt, host num ${hosts[$((${i} % ${num_hosts}))]}, port ${ports[$((${i} % ${num_hosts}))]}"
	nohup python3 client.py xact-files/$((${i} + 1)).txt -hn ${hosts[$((${i} % ${num_hosts}))]} -p ${ports[$((${i} % ${num_hosts}))]} >$((${i} + 1))_output.out 2>$((${i} + 1))_stats.out &
done
