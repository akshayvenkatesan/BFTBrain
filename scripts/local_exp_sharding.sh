# Usage: ./local_exp.sh [protocol] [learning (optional)]
# Examples:
#        ./local_exp.sh pbft # run pbft without learning agents
#        ./local_exp.sh pbft # run bedrock with learning agents, using pbft as the default protocol
# [protocol]: protocol name to run
# [learning]: if *learning* provided, each bedrock entity will be paired with a local learning agent

protocol=$1
start_port=$2
cluster_number=$3
learning=$4

session_name="cloudlab_$cluster_number"
session_learning_name="cloudlab_learning_$cluster_number"

count=6 # 3f+3
agent_count=0
# if learning, agent_count=count-2
if [ $# -gt 1 ]; then
  if [ "$4" == "learning" ]; then
    agent_count=$((count-2))
  fi
fi

# check if session exists
if tmux has-session -t $session_name 2>/dev/null; then
  # session exists - kill it
  tmux kill-session -t $session_name
fi
if tmux has-session -t $session_learning_name 2>/dev/null; then
  # session exists - kill it
  tmux kill-session -t $session_learning_name
fi

# create new session
tmux new-session -d -s $session_name
if [ $agent_count -gt 0 ]; then
  tmux new-session -d -s $session_learning_name
fi

# create one window for each server
for (( i=0; i<$count-1; i++ ))
do
  tmux new-window -t $session_name
done
for (( i=0; i<$agent_count-1; i++ ))
do
  tmux new-window -t $session_learning_name
done


sleep 5

echo "\033[4;34mStart running $protocol with $count servers\033[m"

echo "Protocol $protocol : [0/6] Killing previous processes if needed ..."

for (( i=0; i<$count; i++ ))
do
  echo "Killing Bedrock and learning agent on machine $i ..."
  tmux send-keys -t $session_name:"$i" "cd ~/BFTBrain/code && ../scripts/kill_process_port.sh $(($start_port+$count)) && ../scripts/kill_process_port.sh $(($start_port+$count+20))" C-m
done

echo "Protocol $protocol : [1/6] Starting Coordination Server"
# start server on the first server
tmux send-keys -t $session_name:0 "./run.sh CoordinatorServer -p $start_port -r $protocol -k $cluster_number" C-m

echo "Protocol $protocol : [2/6] Waiting for 10 seconds for coordination server to set up ..."
sleep 5

# start units
for (( i=1; i<$count-1; i++ ))
do
  echo "Protocol $protocol : [3/6] Starting Coordination Unit $(($i-1))"
  tmux send-keys -t $session_name:"$i" "./run.sh CoordinatorUnit -u $(($i-1)) -p $(($start_port+$i)) -n 1 -s 127.0.0.1:$start_port" C-m
  sleep 1
done

# start learning agents
for (( i=0; i<$agent_count; i++ ))
do
  echo "Protocol $protocol : [3/6] Starting Learning Agent for Coordination Unit $i"
  tmux send-keys -t $session_learning_name:"$i" "cd ~/BFTBrain/code/learning/ && python3 learning_agent.py -u $i -p $(($start_port+1+$i)) -n single" C-m
done
sleep 5

echo "Protocol $protocol : [4/6] Starting Client"
# start client
tmux send-keys -t $session_name:"$(($count-1))" "./run.sh CoordinatorUnit -u $(($count-2)) -p $(($start_port+$count-1)) -c 1 -s 127.0.0.1:$start_port" C-m

echo "Protocol $protocol : [5/6] Waiting for 10 seconds for connection to set up ..."
sleep 5

# coordination server start
tmux send-keys -t $session_name:0 C-m

echo "Protocol $protocol : [6/6] Executing ..."

echo "WAITING FOR INPUT TO KILL THE INSTANCE"
cat

for (( i=0; i<$count; i++ ))
do
  echo "Stopping Bedrock on machine $i ..."
  tmux send-keys -t $session_name:"$i" C-c
done
for (( i=0; i<$agent_count; i++ ))
do
  tmux send-keys -t $session_learning_name"$i" C-c
done

echo "Protocol $protocol : [Finish]"
