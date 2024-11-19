package com.gbft.framework.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.ConcurrentSkipListSet;

import com.gbft.framework.core.Dataset;
import com.gbft.framework.core.Entity;
import com.gbft.framework.data.MessageData;
import com.gbft.framework.data.RequestData;
import com.gbft.framework.statemachine.StateMachine;
import com.gbft.framework.utils.Printer;
import com.gbft.framework.utils.Printer.Verbosity;
import org.apache.commons.lang3.tuple.Pair;

public class CheckpointData {
    private long num;
    private Entity entity;

    // seqnum -> state (state machine)
    private Map<Pair<Long,Long>, Integer> stateMap;
    private Map<Long, RequestData> requests;
    private Map<Pair<Long,Long>, List<RequestData>> requestBlocks;
    // clusternum -> aggregation values
    private Map<Long, Map<Long, NavigableSet<Long>>> aggregationValues;
    private MessageTally messageTally;
    private MessageTally viewTally;
    
    // service state
    protected Dataset serviceState;

    // counter for next(i)
    protected Map<String, LongAdder> decisionMatching;
    private static int decisionQuorumSize = 1;

    // protocol
    protected AtomicReference<String> protocol = new AtomicReference<>();

    // performance
    public long beginTimestamp;
    public float throughput;

    protected Map<Pair<Long,Long>, Map<Long, Integer>> replies;

    public CheckpointData(long num, Entity entity) {
        this.num = num;
        this.entity = entity;
        stateMap = new ConcurrentHashMap<>();
        requests = new ConcurrentHashMap<>();
        requestBlocks = new ConcurrentHashMap<>();
        aggregationValues = new ConcurrentHashMap<>();
        for (long clusterNum = 0; clusterNum < 5; clusterNum++) {
            aggregationValues.put(clusterNum, new ConcurrentHashMap<>());
        }
        messageTally = new MessageTally();
        viewTally = new MessageTally();
        serviceState = null;
        replies = new HashMap<>();
        decisionMatching = new ConcurrentHashMap<>();
    }

    /**
     * Clone dataset, invoked in Node.java (Node, not Client)
     * Only invoke when (seqnum+1) % checkpointSize == 0
     * @param currentServiceState current dataset
     */
    public void setServiceState(Dataset currentServiceState) {
        this.serviceState = new Dataset(currentServiceState);
    }

    public Dataset getServiceState() {
        return serviceState;
    }

    public void tally(MessageData message) {
        messageTally.tally(message);
        viewTally.tally(message.toBuilder().clearDigest().build());

        // track number of received messages per slot
        var seqnum = message.getSequenceNum();
        if (seqnum == entity.getBeginOfEpisode(seqnum)) {
            entity.getFeatureManager().count(entity.getEpisodeNum(seqnum), FeatureManager.RECEIVED_MESSAGE_PER_SLOT);
        }

        // timestamp when receiving leader proposal
        var type = message.getMessageType();
        if (StateMachine.messages.get(type).hasRequestBlock && type != StateMachine.REQUEST && type != StateMachine.REPLY) {
            entity.getFeatureManager().received(entity.getEpisodeNum(seqnum), seqnum);
        }

        if (type == StateMachine.REPLY && !message.getSwitch().getNextProtocol().isEmpty()) {
            decisionMatching.computeIfAbsent(message.getSwitch().getNextProtocol(), p -> new LongAdder()).increment();
        }

        // used to debug tally stack trace
        if (Printer.verbosity >= Verbosity.VVVVVV) {
            StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();

            var sb = new StringBuilder("");
            for (var element : stackTraceElements) {
                sb.append(" class: " + element.getClassName() + " file: " + element.getFileName() + " line: " + element.getLineNumber() + " method: " + element.getMethodName() + "\n");
            }

            Printer.print(Verbosity.VVVVVV, "Tally stacktrace ", sb.toString().trim());
        }
    }

    public void tallyDecision(String decision) {
        decisionMatching.computeIfAbsent(decision, p -> new LongAdder()).increment();
    }

    public void addAggregationValue(MessageData message) {
        var seqnum = message.getSequenceNum();
        var clusternum = -1L;
        if (!message.getAggregationValuesList().isEmpty()) {
            if(this.entity.isClient())
            {
                clusternum = message.getSource()/4L;
            }
            else
            {
                clusternum = this.entity.getId()/4L;
            }
            aggregationValues.computeIfAbsent(clusternum, v-> v.computeIfAbsent(seqnum, k -> new ConcurrentSkipListSet<>())).addAll(message.getAggregationValuesList());
        }
    }

    public void addAggregationValue(long clusternum, long seqnum, Set<Long> values) {
        var clusternum = -1L;
        if (!values.isEmpty()) {
            aggregationValues.computeIfAbsent(clusternum, v-> v.computeIfAbsent(seqnum, k -> new ConcurrentSkipListSet<>())).addAll(values);
        }
    }

    public NavigableSet<Long> getAggregationValues(long clusternum, long seqnum) {
        aggregationValues.putIfAbsent(seqnum, new ConcurrentHashMap<>());
        return aggregationValues.get(clusternum).getOrDefault(seqnum, new ConcurrentSkipListSet<>());
    }

    public String getDecision() {
        Optional<String> nextProtocol;
        do {
            nextProtocol = decisionMatching.entrySet().parallelStream()
                    .filter(entry -> (entry.getValue().longValue() >= decisionQuorumSize)).map(entry -> entry.getKey())
                    .findAny();
        } while (!nextProtocol.isPresent());

        return nextProtocol.get();
    }

    public void addRequestBlock(Pair<Long,Long> pair_clusternum_seqnum, List<RequestData> requestBlock) {
        requestBlock.forEach(request -> requests.put(request.getRequestNum(), request));
        requestBlocks.put(pair_clusternum_seqnum, requestBlock);
        // stateMap.put(seqnum, StateMachine.IDLE);
    }

    public RequestData getRequest(long reqnum) {
        return requests.get(reqnum);
    }

    public List<RequestData> getRequestBlock(Pair<Long,Long> pair_clusternum_seqnum) {
        return requestBlocks.get(pair_clusternum_seqnum);
    }

    public void addReplies(Pair<Long,Long> pair_clusternum_seqnum, Map<Long, Integer> blockReplies) {
        replies.put(pair_clusternum_seqnum, blockReplies);
    }

    public Map<Long, Integer> getReplies(Pair<Long,Long> pair_clusternum_seqnum) {
        return replies.get(pair_clusternum_seqnum);
    }

    public MessageTally getMessageTally() {
        return messageTally;
    }

    public MessageTally getViewTally() {
        return viewTally;
    }

    /**
     * Set state of a seqnum to state
     * 
     * Set at `transition` and `execute` in `Entity.java`
     * Updated in `StateUpdateLoop` in `Entity.java`
     * @param seqnum sequence number
     * @param state state
     */
    public void setState(Pair<Long,Long> pair_clusternum_seqnum, int state) {
        stateMap.put(pair_clusternum_seqnum, state);
    }

    /**
     * Get state of a seqnum
     * @param seqnum sequence number
     * @return state
     */
    public int getState(Pair<Long, Long> pair_clusternum_seqnum) {
        if (protocol.get() == null) {
            return StateMachine.ANY_STATE;
        } else {
            System.out.println("stateMap: " + stateMap);
            return stateMap.getOrDefault(pair_clusternum_seqnum, StateMachine.states.indexOf(StateMachine.findState("idle", protocol.get() + "_")));
        }
    }

    public long getNum() {
        return num;
    }

    public void setProtocol(String protocol) {
        this.protocol.set(protocol);
    }

    public String getProtocol() {
        return protocol.get();
    }
}
