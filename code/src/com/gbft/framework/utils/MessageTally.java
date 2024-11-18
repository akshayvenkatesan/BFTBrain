package com.gbft.framework.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.lang3.tuple.Pair;

import com.gbft.framework.data.MessageData;
import com.gbft.framework.data.RequestData;
import com.gbft.framework.statemachine.StateMachine;
import com.gbft.framework.utils.MessageTally.QuorumId;
import com.gbft.framework.utils.Printer.Verbosity;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.tuple.Pair;

public class MessageTally {

    // seqnum -> message-type -> view-num -> digest -> set(node)
    protected Map<Pair<Long,Long>, Map<Integer, ConcurrentSkipListMap<Long, Map<ByteString, Set<Integer>>>>> counter;

    // sequence-num -> message-type -> view-num
    protected Map<Pair<Long,Long>, Map<QuorumId, ConcurrentSkipListSet<Long>>> quorumMessages;

    // sequence-num -> view-num -> digest
    protected Map<Pair<Long,Long>, ConcurrentSkipListMap<Long, ByteString>> quorumDigests;

    protected Map<ByteString, List<RequestData>> candidateBlocks;
    protected Map<ByteString, Map<Long, Integer>> candidateReplies;

    protected ReadLock counterReadLock;
    protected WriteLock counterWriteLock;
    protected ReadLock quorumReadLock;
    protected WriteLock quorumWriteLock;

    public MessageTally() {
        counter = new ConcurrentHashMap<>();
        quorumMessages = new ConcurrentHashMap<>();
        quorumDigests = new ConcurrentHashMap<>();
        candidateBlocks = new ConcurrentHashMap<>();
        candidateReplies = new ConcurrentHashMap<>();

        var counterLock = new ReentrantReadWriteLock();
        counterReadLock = counterLock.readLock();
        counterWriteLock = counterLock.writeLock();

        var quorumLock = new ReentrantReadWriteLock();
        quorumReadLock = quorumLock.readLock();
        quorumWriteLock = quorumLock.writeLock();
    }

    public void tally(MessageData message) {
        var seqnum = message.getSequenceNum();
        var viewnum = message.getViewNum();
        var digest = message.getDigest();
        var source = message.getSource();
        var clusternum = source / 4;
        var pair_clusternum_seqnum = Pair.of((long) clusternum, seqnum);
        var type = message.getMessageType();
        System.out.println("Tallying message: " + message.toString());
        counterWriteLock.lock();

        if (StateMachine.messages.get(type).hasRequestBlock) {
            candidateBlocks.put(digest, message.getRequestsList());
        }

        if (!message.getReplyDataMap().isEmpty()) {
            candidateReplies.put(digest, message.getReplyDataMap());
        }

        counter.computeIfAbsent(pair_clusternum_seqnum, s -> new ConcurrentHashMap<>())
               .computeIfAbsent(type, t -> new ConcurrentSkipListMap<>())
               .computeIfAbsent(viewnum, v -> new ConcurrentHashMap<>())
               .computeIfAbsent(digest, d -> ConcurrentHashMap.newKeySet())
               .add(source);

        counterWriteLock.unlock();
    }

    public Long getMaxQuorum(Pair<Long,Long> pair_clusternum_seqnum, QuorumId quorumId) {
        Long max = null;
        counterReadLock.lock();
        var subcounter = DataUtils.nestedGetPair(counter, pair_clusternum_seqnum, quorumId.message); // subcounter.key = view
        // print all elements of coounter
        // System.out.println("Seqnum: " + seqnum + " QuorumId: " + quorumId.message);
        // System.out.println("Printing all elements of counter");
        //Print all elements of counter
        // for (Map.Entry<Long, Map<Integer, ConcurrentSkipListMap<Long, Map<ByteString, Set<Integer>>>>> seqEntry : counter.entrySet()) {
        //     // System.out.println("Seqnum: " + seqEntry.getKey());
        //     for (Map.Entry<Integer, ConcurrentSkipListMap<Long, Map<ByteString, Set<Integer>>>> typeEntry : seqEntry.getValue().entrySet()) {
        //     System.out.println("MessageType: " + typeEntry.getKey());
        //     for (Map.Entry<Long, Map<ByteString, Set<Integer>>> viewEntry : typeEntry.getValue().entrySet()) {
        //         System.out.println("Viewnum: " + viewEntry.getKey());
        //         for (Map.Entry<ByteString, Set<Integer>> digestEntry : viewEntry.getValue().entrySet()) {
        //         System.out.println("Digest: " + digestEntry.getKey());
        //         for (Integer node : digestEntry.getValue()) {
        //             System.out.println("Node: " + node);
        //         }
        //         }
        //     }
        //     }
        // }
        if (subcounter == null) {
            System.out.println("subcounter is null");
            counterReadLock.unlock();
            return null;
        }

        quorumReadLock.lock();
        var subquorum = DataUtils.nestedGetPair(quorumMessages, pair_clusternum_seqnum, quorumId);
        var currentmax = subquorum == null ? -1L : subquorum.last();
        if (subcounter.lastKey() <= currentmax) {
            max = subquorum.last();
            quorumReadLock.unlock();
            counterReadLock.unlock();

            if (Printer.verbosity >= Verbosity.VVVVV) {
                // Printer.print(Verbosity.VVVVV, "GetMaxQuorum Success [currentmax]: ", StateMachine.messages.get(quorumId.message).name.toUpperCase() + " seqnum: " + seqnum + " size: " + quorumId.quorum);
            }

            return max;
        }
        quorumReadLock.unlock();

        quorumWriteLock.lock();
        for (var viewnum : subcounter.tailMap(currentmax, false).descendingKeySet()) {
            var digest = subcounter.get(viewnum).entrySet().parallelStream()
                                   .filter(entry -> entry.getValue().size() >= quorumId.quorum)
                                   .map(entry -> entry.getKey()).findAny();
            if (digest.isPresent()) {
                updateQuorumDigest(pair_clusternum_seqnum, viewnum, digest.get(), quorumId);
                max = viewnum;

                if (Printer.verbosity >= Verbosity.VVVVV) {
                    var q = DataUtils.nestedGet(subcounter, max, digest.get());
                    StringBuilder sb = new StringBuilder();
                    sb.append("[");
                    for(Integer x : q) {
                        sb.append(Integer.toString(x));
                        sb.append(" ");
                    }
                    String st = sb.toString();
                    st = st.trim();
                    st = st + "]";

                    // Printer.print(Verbosity.VVVVV, "GetMaxQuorum Success [filter]: ", StateMachine.messages.get(quorumId.message).name.toUpperCase() + " seqnum: " + seqnum + " size: " + quorumId.quorum + " quorum: " + st);
                }
                break;
            }
        }
        quorumWriteLock.unlock();
        counterReadLock.unlock();
        System.out.println("Returning from here2 is null");
        return max;
    }

    public boolean hasQuorum(Pair<Long,Long> pair_clusternum_seqnum, long viewnum, QuorumId quorumId) {
        counterReadLock.lock();
        var subcounter = DataUtils.nestedGetPair(counter, pair_clusternum_seqnum, quorumId.message); // subcounter.key = view
        if (subcounter == null || !subcounter.containsKey(viewnum)) {
            counterReadLock.unlock();
            return false;
        }

        quorumReadLock.lock();
        var subquorum = DataUtils.nestedGetPair(quorumMessages, pair_clusternum_seqnum, quorumId);
        if (subquorum != null && subquorum.contains(viewnum)) {
            quorumReadLock.unlock();
            counterReadLock.unlock();
            return true;
        }
        quorumReadLock.unlock();

        quorumWriteLock.lock();
        var viewcounter = subcounter.get(viewnum);
        var digest = viewcounter.entrySet().parallelStream()
                                .filter(entry -> entry.getValue().size() >= quorumId.quorum)
                                .map(entry -> entry.getKey()).findAny();

        var match = digest.isPresent();
        if (match) {
            updateQuorumDigest(pair_clusternum_seqnum, viewnum, digest.get(), quorumId);
        }
        quorumWriteLock.unlock();
        counterReadLock.unlock();

        return match;
    }

    public void updateQuorumBlock(Pair<Long,Long> pair_clusternum_seqnum, long viewnum, ByteString digest, List<RequestData> block, QuorumId quorumId) {
        quorumWriteLock.lock();
        candidateBlocks.put(digest, block);
        updateQuorumDigest(pair_clusternum_seqnum, viewnum, digest, quorumId);
        quorumWriteLock.unlock();
    }

    private void updateQuorumDigest(Pair<Long,Long> pair_clusternum_seqnum, long viewnum, ByteString digest, QuorumId quorumId) {
        quorumDigests.computeIfAbsent(pair_clusternum_seqnum, s -> new ConcurrentSkipListMap<>()).put(viewnum, digest);
        quorumMessages.computeIfAbsent(pair_clusternum_seqnum, s -> new ConcurrentHashMap<>())
                      .computeIfAbsent(quorumId, c -> new ConcurrentSkipListSet<>()).add(viewnum);
    }

    public Set<Integer> getQuorumNodes(Pair<Long,Long> pair_clusternum_seqnum, long viewnum, QuorumId quorumId) {
        quorumReadLock.lock();
        var digest = getQuorumDigest(pair_clusternum_seqnum, viewnum);
        quorumReadLock.unlock();

        counterReadLock.lock();
        var nodes = counter.get(pair_clusternum_seqnum).get(quorumId.message).get(viewnum).get(digest);
        counterReadLock.unlock();

        return nodes;
    }

    public ByteString getQuorumDigest(Pair<Long,Long> pair_clusternum_seqnum, long viewnum) {
        var submap = quorumDigests.get(pair_clusternum_seqnum);
        return submap == null ? null : submap.get(viewnum);
    }

    public List<RequestData> getQuorumBlock(Pair<Long,Long> pair_clusternum_seqnum, long viewnum) {
        quorumReadLock.lock();
        var submap = quorumDigests.get(pair_clusternum_seqnum);
        quorumReadLock.unlock();
        return submap == null ? null : candidateBlocks.get(submap.get(viewnum));
    }

    public Map<Long, Integer> getQuorumReplies(Pair<Long,Long> pair_clusternum_seqnum, long viewnum) {
        var submap = quorumDigests.get(pair_clusternum_seqnum);
        return submap == null ? null : candidateReplies.get(submap.get(viewnum));
    }

    public Long getMaxQuorum(Pair<Long,Long> pair_clusternum_seqnum) {
        var submap = quorumDigests.get(pair_clusternum_seqnum);
        return submap == null ? null : submap.lastKey();
    }

    public static class QuorumId {
        public int message;
        public int quorum;

        public QuorumId(int message, int quorum) {
            this.message = message;
            this.quorum = quorum;
        }

        @Override
        public int hashCode() {
            return Objects.hash(message, quorum);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            QuorumId other = (QuorumId) obj;
            return message == other.message && quorum == other.quorum;
        }
    }
}
