package com.gbft.framework.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.commons.lang3.tuple.Pair;

import com.gbft.framework.core.Entity;
import com.gbft.framework.statemachine.StateMachine;
import com.google.protobuf.ByteString;

public class CheckpointManager {

    private Entity entity;
    private int CHECKPOINT;
    private int checkpointSize;
    private ConcurrentSkipListMap<Pair<Long,Long>, CheckpointData> checkpoints;
    private long lastStableCheckpoint; // s
    private long lowWaterMark; // h
    public final int lowHighGap; // k

    public CheckpointManager(Entity entity) {
        this.entity = entity;

        CHECKPOINT = StateMachine.messages.indexOf(StateMachine.findMessage("checkpoint"));
        checkpointSize = Config.integer("benchmark.checkpoint-size");
        lowHighGap = Config.integer("benchmark.catch-up-k");
        lastStableCheckpoint = -1;
        lowWaterMark = 0;

        checkpoints = new ConcurrentSkipListMap<>();
        for(int i =0;i<5;i++) {
            checkpoints.put(Pair.of((long) i, 0L), new CheckpointData(0, this.entity));
        }
    }

    /**
     * Send Checkpoint Message
     * @param checkpointNum Sequence Number / Checkpoint Size
     */
    public void sendCheckpoint(Pair<Long,Long> checkpointNum) {
        // System.out.println(entity.prefix + "sendCheckpoint, checkpointNum:  " + checkpointNum);

        var digest = getCheckpointDigest(checkpointNum);
        var targets = entity.getRolePlugin().getRoleEntities(0, 0, StateMachine.NORMAL_PHASE, StateMachine.NODE, 1);
        var message = DataUtils.createMessage(checkpointNum.getRight(), 0L, CHECKPOINT, entity.getId(), targets, List.of(),
                entity.EMPTY_BLOCK, null, digest);
        entity.sendMessage(message);       
    } 

    public ByteString getCheckpointDigest(Pair<Long,Long> checkpointNum) {
        var stream = new ByteArrayOutputStream();
        try {
            var checkpoint = getCheckpoint(checkpointNum);
            for (var entry : checkpoint.serviceState.getRecords().entrySet()) {
                stream.write(entry.getKey().byteValue());
                stream.write(entry.getValue().byteValue());
            }

            stream.flush();
            stream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return DataUtils.getDigest(stream.toByteArray());
    }

    public ByteString getCheckpointDigest(Map<Integer, Integer> state) {
        var stream = new ByteArrayOutputStream();
        Map<Integer, Integer> sorted_state = new TreeMap<>();
        state.forEach((key, value) -> sorted_state.put(key, value));

        try {
            for (var entry : sorted_state.entrySet()) {
                stream.write(entry.getKey().byteValue());
                stream.write(entry.getValue().byteValue());
            }

            stream.flush();
            stream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return DataUtils.getDigest(stream.toByteArray());
    }

    public void setLastStableCheckpoint(long checkpointNum) {
        lastStableCheckpoint = checkpointNum;
    }

    public Long getLastStableCheckpoint() {
        return lastStableCheckpoint;
    }

    public void setLowWaterMark(long h) {
        lowWaterMark = h;
    }

    public long getLowWaterMark() {
        return lowWaterMark;
    }

    public Long getMinCheckpoint() {
        return checkpoints.firstKey().getValue();
    }

    public Long getMaxCheckpoint() {
        return checkpoints.lastKey().getValue();
    }

    public CheckpointData getCheckpoint(Pair<Long,Long> checkpointNum) {
        return checkpoints.computeIfAbsent(checkpointNum, num -> new CheckpointData(num.getRight(), this.entity));
    }

    public void removeCheckpoint(Pair<Long,Long> checkpointNum) {
        checkpoints.remove(checkpointNum);
    }

    public Pair<Long,Long> getCheckpointNum(long clusternum, long seqnum) {
        return Pair.of(clusternum, seqnum / checkpointSize);
    }

    /**
     * Everything tallied in checkpoint
     * @param seqnum sequence number
     * @return checkpoint that is responsible for seqnum
     */
    public CheckpointData getCheckpointForSeq(long clusternum, long seqnum) {
        return getCheckpoint(Pair.of(clusternum, seqnum / checkpointSize));
    }

    public CheckpointData getPrevCheckpointForSeq(long clusternum, long seqnum) {
        return getCheckpoint(Pair.of(clusternum, seqnum/checkpointSize - 1));
    }
}
