package com.gbft.framework.core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import com.gbft.framework.coordination.CoordinatorUnit;
import com.gbft.framework.data.RequestData;
import com.gbft.framework.statemachine.StateMachine;
import com.gbft.framework.statemachine.Transition.UpdateMode;
import com.gbft.framework.utils.AdvanceConfig;
import com.gbft.framework.utils.BenchmarkManager;
import com.gbft.framework.utils.Config;
import com.gbft.framework.utils.MessageTally.QuorumId;
import com.gbft.framework.utils.Printer;
import com.gbft.framework.utils.Printer.Verbosity;

public class ShardingClient extends Entity {

    protected long nextRequestNum;
    protected long intervalns;
    protected final int requestTargetRole;

    protected ClientDataset dataset;

    private RequestGenerator requestGenerator;

    public ShardingClient(int id, CoordinatorUnit coordinator) {
        super(id, coordinator);

        intervalns = Config.integer("benchmark.request-interval-micros") * 1000L;
        var targetConfig = Config.string("protocol.general.request-target");
        requestTargetRole = StateMachine.roles.indexOf(targetConfig);

        System.out.println("Creating client dataset for sharding client " + id + ".");
        dataset = new ClientDataset(id);
        nextRequestNum = 0L;

        requestGenerator = createRequestGenerator();
        requestGenerator.init();
    }

    protected RequestGenerator createRequestGenerator() {
        return new RequestGenerator();
    }

    @Override
    protected void execute(long seqnum) {
        System.out.println("Inside execute for sharding client " + id + " with seqnum " + seqnum + ".");
        var checkpoint = checkpointManager.getCheckpointForSeq(seqnum);

        var tally = checkpoint.getMessageTally();
        var viewnum = tally.getMaxQuorum(seqnum);
        var replies = tally.getQuorumReplies(seqnum, viewnum);
        currentViewNum = viewnum;
        /*
         * Checks for replies for the requests in the block and updates the dataset.
         * Lookahead is when sending the request, and client dataset is updated on replies
         */
        if (replies != null) {
            System.out.println("Updating dataset for sharding client " + id + ".");
            var now = System.nanoTime();
            for (var entry : replies.entrySet()) {
                var reqnum = entry.getKey();
                var request = checkpoint.getRequest(reqnum);
                dataset.update(request, entry.getValue());

                // benchmarkManager.requestExecuted(reqnum, now);
            }
            /*
             * this mainly releases one semaphore to ensure only
             * one request is active at a time. Because each request
             * simulates a client, the client is blocked until the
             * next simulation is started
             */
            requestGenerator.execute();

        }
    }

    @Override
    public boolean isClient() {
        return true;
    }

    public class RequestGenerator {
        protected final Semaphore semaphore = new Semaphore(1);

        public void init() {
            threads.add(new Thread(new RequestGenerator.RequestGeneratorRunner()));
        }

        protected class RequestGeneratorRunner implements Runnable {
            @Override
            public void run() {
                try{
                    while (running) {
                        semaphore.acquire();
                        var next = System.nanoTime() + intervalns;
    
                        var request = dataset.createRequest(nextRequestNum);
    
                        var clusternum = request.getRecord() / 25 + 1; 
                        nextRequestNum += 1;
    
                        sendRequest(request, clusternum);
    
                        while (System.nanoTime() < next) {
                            LockSupport.parkNanos(intervalns / 3);
                        }
                    }
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                
            }
        }

        protected void sendRequest(RequestData request , int clusternum) {
            var reqnum = request.getRequestNum();
            var seqnum = reqnum / blockSize;
            var view = currentViewNum;

            // wait to know the leader mode if necessary
            var episode = getEpisodeNum(seqnum);
            rolePlugin.roleReadLock.lock();
            try {
                if (rolePlugin.episodeLeaderMode.get(episode) == null) {
                    rolePlugin.roleReadLock.unlock();
                    rolePlugin.roleWriteLock.lock();
                    try {
                        while (rolePlugin.episodeLeaderMode.get(episode) == null) {
                            rolePlugin.roleCondition.await();
                        }
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    } finally {
                        rolePlugin.roleWriteLock.unlock();
                        rolePlugin.roleReadLock.lock();
                    }
                }
            } finally {
                rolePlugin.roleReadLock.unlock();
            }

            // Identify primary and send request
            var targets = rolePlugin.getRoleEntities(seqnum, view, StateMachine.NORMAL_PHASE, requestTargetRole, clusternum);

            if (request.getOperationValue() == RequestData.Operation.READ_ONLY_VALUE) {
                targets = rolePlugin.getRoleEntities(seqnum, view, StateMachine.NORMAL_PHASE, StateMachine.NODE, clusternum);
            }

            var message = createMessage(null, view, List.of(request), StateMachine.REQUEST, id, targets);
            System.out.println("Sending message for sharding client " + id + ".");
            sendMessage(message);

            if (Printer.verbosity >= Printer.Verbosity.VVV) {
                Printer.print(Printer.Verbosity.VVV, prefix, "Request created: ", request);
            }
        }

        protected void execute() {
            System.out.println("Release semaphore for sharding client " + id + ".");
            semaphore.release();
        }
    }
}

