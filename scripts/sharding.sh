#!/bin/bash

# Usage: ./sharding.sh [protocol] [learning (optional)]
if [ "$#" -lt 1 ]; then
    echo "Usage: $0 [protocol] [learning (optional)]"
    exit 1
fi

protocol=$1
learning=$2

starting_port=6020
cluster_number=1
cd ../code
echo $PWD
tmux new-session -d -s "shard_coordinator"
tmux send-keys -t shard_coordinator "./run.sh CoordinatorServer -p 5050 -r $protocol -k 0" C-m


echo "Waiting for 10 seconds for ShardCoordinator to set up ..."
sleep 5


cd ../scripts
# Loop to call local_exp_sharding.sh four times
for i in {0..3}
do
    session_name="sharding_${protocol}_${cluster_number}"
    echo "Starting instance $i: $protocol with starting port: $starting_port and cluster number: $cluster_number in tmux session: $session_name"
    
    # Create a new tmux session for each instance
    tmux new-session -d -s "$session_name"
    
    if [ ! -z "$learning" ]; then
        command_to_run="./local_exp_sharding.sh $i $protocol $starting_port $cluster_number $learning"
    else
        command_to_run="./local_exp_sharding.sh $i $protocol $starting_port $cluster_number"
    fi
    
    # Send the command to the tmux session
    tmux send-keys -t "$session_name" "$command_to_run" C-m

    starting_port=$((starting_port + 1000))
    cluster_number=$((cluster_number + 1))
done

cd ../code
sleep 5
echo "Starting Client"
tmux new-session -d -s "sharding_client"
tmux send-keys -t sharding_client "./run.sh CoordinatorUnit -u 16 -p 5051 -c 1 -s 127.0.0.1:5050 -k 0" C-m

sleep 5
tmux send-keys -t shard_coordinator:0 C-m



echo "All instances started in their respective tmux sessions."
echo "Use 'tmux attach-session -t session_name' to attach to a session."
