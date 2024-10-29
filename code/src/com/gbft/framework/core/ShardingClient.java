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

    // Map of txid -> cluster -> responses
    private Map<Long, Map<Integer, Integer>> responses;

    private Map<Long, Map<Integer, Integer>> rollbackResponses;

    private Map<Long, int[]> rollbackTransactions;

    public ShardingClient(int id, CoordinatorUnit coordinator) {
        super(id, coordinator);

        intervalns = Config.integer("benchmark.request-interval-micros") * 1000L;
        var targetConfig = Config.string("protocol.general.request-target");
        requestTargetRole = StateMachine.roles.indexOf(targetConfig);

        System.out.println("Creating client dataset for sharding client " + id + ".");
        dataset = new ClientDataset(id);
        nextRequestNum = 1L;
        responses = new HashMap<>();
        rollbackResponses = new HashMap<>();
        rollbackTransactions = new HashMap<>();
        requestGenerator = createRequestGenerator();
        requestGenerator.init();
    }

    protected RequestGenerator createRequestGenerator() {
        return new RequestGenerator();
    }

    // @Override
    // protected void execute(long seqnum) {
    //     System.out.println("Inside execute for sharding client " + id + " with seqnum " + seqnum + ".");
    //     var checkpoint = checkpointManager.getCheckpointForSeq(seqnum);

    //     var tally = checkpoint.getMessageTally();
    //     var viewnum = tally.getMaxQuorum(seqnum);
    //     var replies = tally.getQuorumReplies(seqnum, viewnum);
    //     currentViewNum = viewnum;
    //     /*
    //      * Checks for replies for the requests in the block and updates the dataset.
    //      * Lookahead is when sending the request, and client dataset is updated on replies
    //      */
    //     if (replies != null) {
    //         System.out.println("Updating dataset for sharding client " + id + ".");
    //         var now = System.nanoTime();
    //         for (var entry : replies.entrySet()) {
    //             var reqnum = entry.getKey();
    //             var request = checkpoint.getRequest(reqnum);
    //             //Check if both the replies are as expected, if not create and execute rollback transaction
    //             //After previous logic, decrease inorder of associated transactions. If inorder==0, add them to queue
    //             dataset.update(request, entry.getValue());

    //             // benchmarkManager.requestExecuted(reqnum, now);

    //         //Check each currently executing transaction in a map
    //         }
    //         /*
    //          * this mainly releases one semaphore to ensure only
    //          * one request is active at a time. Because each request
    //          * simulates a client, the client is blocked until the
    //          * next simulation is started
    //          */
    //         requestGenerator.execute();

    //     }
    // }
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
                int record = request.getRecord();
                var clusterNum = record / 25 + 1;
                System.out.println("*"); 
                System.out.println("*"); 
                System.out.println("*"); 
                System.out.println("*"); 
                System.out.println("Key: "+record+" Value: "+entry.getValue()); 
                System.out.println("*"); 
                System.out.println("*"); 
                System.out.println("*"); 
                System.out.println("*");

                if (rollbackResponses.containsKey(reqnum)) {
                    // This is a response for a rollback request

                    var responseMap = rollbackResponses.get(reqnum);
                    responseMap.put(clusterNum, responseMap.getOrDefault(clusterNum, 0) + 1);
                } else if (entry.getValue() < 0) {
                    // We need to send a rollback as the balance has become negative
                    rollbackResponses.put(reqnum, new HashMap<>());
                    var rollbackTx = rollbackTransactions.get(reqnum);
                    var firstRollbackRequest = dataset.createRequestWithKeyAndVal(nextRequestNum++, rollbackTx[0], rollbackTx[1]);
                    sendRequest(firstRollbackRequest, clusterNum);
                    var nextRollback = reqnum % 10 == 1 ? 2 : 1;
                    var newRequestNum = Long.valueOf(reqnum / 10 + String.valueOf(nextRollback));
                    rollbackResponses.put(newRequestNum, new HashMap<>());
                    var nextRollbackTx = rollbackTransactions.get(newRequestNum);
                    var secondRollbackRequest = dataset.createRequestWithKeyAndVal(nextRequestNum++, nextRollbackTx[0], nextRollbackTx[1]);
                    var nextClusterNum = (int) (newRequestNum / 25 + 1);
                    sendRequest(secondRollbackRequest, nextClusterNum);
                } else {
                    //Check if both the replies are as expected, if not create and execute rollback transaction
                    //After previous logic, decrease inorder of associated transactions. If inorder==0, add them to queue
                    dataset.update(request, entry.getValue());
                }

                // benchmarkManager.requestExecuted(reqnum, now);

            //Check each currently executing transaction in a map
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

                        // generating 1000 random transactions.
                        List<int[]> transactions = dataset.generateRandomTransactions(1);
                        for (int i =0;i<transactions.size();i++) {
                            int[] currentTransaction = transactions.get(i);
                            // If we are depositing in the same account then simply add the val to that key
                            System.out.println("Sender is : " + currentTransaction[0]);
                            System.out.println("Receiver is : " + currentTransaction[1]);
                            System.out.println("Amount is : " + currentTransaction[2]);
                            System.out.println(" Txn number is : " + nextRequestNum);
                            if (currentTransaction[0] == currentTransaction[1]) {
                                var request = dataset.createRequestWithKeyAndVal(nextRequestNum, currentTransaction[0], currentTransaction[2]);
                                nextRequestNum += 1;
                                var clusternum0 = request.getRecord() / 25 + 1;
                                sendRequest(request, clusternum0);
                            } else {
                                var requestNum = String.valueOf(nextRequestNum);
                                Long firstRequestNumber = Long.valueOf(requestNum + '1');
                                Long secondRequestNumber = Long.valueOf(requestNum + '2');
                                rollbackTransactions.putIfAbsent(firstRequestNumber, new int[]{currentTransaction[0], currentTransaction[2]});
                                rollbackTransactions.putIfAbsent(secondRequestNumber, new int[]{currentTransaction[1], -currentTransaction[2]});
                                responses.putIfAbsent(firstRequestNumber, new HashMap<>());
                                responses.putIfAbsent(secondRequestNumber, new HashMap<>());
                                for (int clusterNum = 1; clusterNum <= 4; clusterNum++) {
                                    responses.get(firstRequestNumber).put(clusterNum, 0);
                                    responses.get(secondRequestNumber).put(clusterNum, 0);
                                }
                                // Sending first request with a-val
                                System.out.println(" First request number is : " + firstRequestNumber);
                                System.out.println(" Second request number is : " + secondRequestNumber);
                                var request1 = dataset.createRequestWithKeyAndVal(firstRequestNumber, currentTransaction[0], -currentTransaction[2]);
                                var clusternum1 = request1.getRecord() / 25 + 1;
                                sendRequest(request1, clusternum1);
                                while (System.nanoTime() < next) {
                                    LockSupport.parkNanos(intervalns / 3);
                                }
                                // Sending second request with b+val
                                var request2 = dataset.createRequestWithKeyAndVal(secondRequestNumber, currentTransaction[1], +currentTransaction[2]);
                                nextRequestNum += 1;
                                var clusternum2 = request2.getRecord() / 25 + 1;
                                sendRequest(request2, clusternum2);
                            }
                        }
//                    var request = dataset.createRequest(nextRequestNum);
//                    nextRequestNum += 1;
//
//                    sendRequest(request);


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
            var seqnum = reqnum; // blockSize;
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
            //semaphore.release();
        }
    }
}


