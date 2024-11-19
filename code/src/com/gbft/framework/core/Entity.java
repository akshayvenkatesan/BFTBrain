package com.gbft.framework.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import com.gbft.framework.coordination.CoordinatorUnit;
import com.gbft.framework.data.AgentCommGrpc;
import com.gbft.framework.data.LearningData;
import com.gbft.framework.data.MessageData;
import com.gbft.framework.data.RequestData;
import com.gbft.framework.data.SwitchingData;
import com.gbft.framework.data.AgentCommGrpc.AgentCommBlockingStub;
import com.gbft.framework.data.RequestData.Operation;
import com.gbft.framework.fault.InDarkFault;
import com.gbft.framework.fault.PollutionFault;
import com.gbft.framework.fault.SlowProposalFault;
import com.gbft.framework.fault.TimeoutFault;
import com.gbft.framework.plugins.MessagePlugin;
import com.gbft.framework.plugins.PipelinePlugin;
import com.gbft.framework.plugins.PluginManager;
import com.gbft.framework.plugins.RolePlugin;
import com.gbft.framework.plugins.TransitionPlugin;
import com.gbft.framework.statemachine.Condition;
import com.gbft.framework.statemachine.StateMachine;
import com.gbft.framework.statemachine.Transition;
import com.gbft.framework.statemachine.Transition.UpdateMode;
import com.gbft.framework.utils.BenchmarkManager;
import com.gbft.framework.utils.CheckpointManager;
import com.gbft.framework.utils.Config;
import com.gbft.framework.utils.DataUtils;
import com.gbft.framework.utils.EntityMapUtils;
import com.gbft.framework.utils.FeatureManager;
import com.gbft.framework.utils.MessageTally;
import com.gbft.framework.utils.MessageTally.QuorumId;
import com.gbft.framework.utils.Printer;
import com.gbft.framework.utils.Printer.Verbosity;
import com.gbft.plugin.role.BasicPrimaryPlugin;
import com.gbft.plugin.role.PrimaryPassivePlugin;
import com.gbft.framework.utils.Timekeeper;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.tuple.Pair;

import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;

public abstract class Entity {

    // Config

    protected final int blockSize; // The number of requests in each block.
    protected final int checkpointSize; // The number of blocks/sequences in each checkpoint.

    // Properties

    protected final int id;
    public final String prefix; // The entity prefix when printing messages in output.

    // Protocol Data

    protected ConcurrentLinkedQueue<RequestData> pendingRequests;
    protected Map<Long, Long> reqnumToSeqnumMap;
    protected CheckpointManager checkpointManager;

    public final List<RequestData> EMPTY_BLOCK;
    public final ByteString EMPTY_DIGEST;

    public Dataset dataset;

    // Protocol State

    protected long nextSequence;
    protected long lastExecutedSequenceNum;
    protected long currentViewNum;
    protected Timekeeper timekeeper;
    // Execution queue for each cluster
    protected Map<Long, Map<Long, Transition>> executionQueue;

    // Concurrency

    protected Set<Pair<Long, Long>> updating;
    protected TreeSet<Pair<Long, Long>> needsUpdate;
    protected ReentrantLock stateLock;

    // Plugins

    protected RolePlugin rolePlugin;
    protected PipelinePlugin pipelinePlugin;
    protected List<MessagePlugin> messagePlugins;
    protected List<TransitionPlugin> transitionPlugins;

    // Execution

    protected boolean running;
    protected List<Thread> threads;
    protected CoordinatorUnit coordinator;

    // Fault

    protected InDarkFault indarkFault;
    protected TimeoutFault timeoutFault;
    protected SlowProposalFault slowProposalFault;
    protected PollutionFault pollutionFault;

    // Others

    public BenchmarkManager benchmarkManager;

    private Object pendingLock = new Object();
    private long systemStartTime;
    private long proposedRequests = 0;

    private double cumulativeDuration = 0.0;

    // Learning

    protected boolean learning;
    public long reportSequence;
    public long exchangeSequence;
    public final int REPORT;
    public final int REPORT_QUORUM;
    public final int EPISODE_SIZE;
    public AtomicInteger currentEpisodeNum;
    public List<String> protocols;

    // episode -> node -> feature-type -> feature-value
    protected Map<Integer, Map<Integer, Map<Integer, Float>>> reports;
    protected MessageTally reportTally;

    protected FeatureManager featureManager;
    protected EntityCommServer entityCommServer;
    protected AgentCommBlockingStub agentStub;

    public Entity(int id, CoordinatorUnit coordinator) {
        this.id = id;
        this.coordinator = coordinator;

        prefix = "{" + id + "} ";

        blockSize = Config.integer("benchmark.block-size");
        checkpointSize = Config.integer("benchmark.checkpoint-size");

        learning = Config.bool("general.learning");
        reportSequence = Config.integer("general.report-sequence");
        exchangeSequence = Config.integer("general.exchange-sequence");
        REPORT = StateMachine.messages.indexOf(StateMachine.findMessage("report", ""));
        REPORT_QUORUM = StateMachine.parseQuorum("2f + 1");
        // episode length = checkpoint length
        EPISODE_SIZE = Config.integer("benchmark.checkpoint-size");
        currentEpisodeNum = new AtomicInteger(0);
        protocols = Config.list("switching.debug-sequence");
        reports = new ConcurrentHashMap<>();
        reportTally = new MessageTally();

        currentViewNum = 0L;
        nextSequence = 0L;
        lastExecutedSequenceNum = -1L;
        executionQueue = new HashMap<>();

        pendingRequests = new ConcurrentLinkedQueue<>();
        reqnumToSeqnumMap = new ConcurrentHashMap<>();
        checkpointManager = new CheckpointManager(this);

        updating = new HashSet<>();
        needsUpdate = new TreeSet<>();
        stateLock = new ReentrantLock();

        dataset = new Dataset();

        threads = new ArrayList<>();
        timekeeper = new Timekeeper(this);
        threads.add(new Thread(() -> executor()));
        threads.add(new Thread(() -> triggerSlowProposal()));
        threads.add(new Thread(() -> aggStateUpdate()));

        rolePlugin = PluginManager.getRolePlugin(this);
        pipelinePlugin = PluginManager.getPipelinePlugin(this);
        messagePlugins = PluginManager.getMessagePlugins(this);
        transitionPlugins = PluginManager.getTransitionPlugins(this);

        EMPTY_BLOCK = new ArrayList<>();
        EMPTY_DIGEST = DataUtils.getDigest(EMPTY_BLOCK);

        benchmarkManager = new BenchmarkManager(this);
        featureManager = new FeatureManager(this);
        running = true;

        indarkFault = new InDarkFault();
        timeoutFault = new TimeoutFault();
        slowProposalFault = new SlowProposalFault();
        pollutionFault = new PollutionFault();

        checkpointManager.getCheckpoint(Pair.of(this.id/4L,0L)).setProtocol(coordinator.defaultProtocol);
        checkpointManager.getCheckpoint(Pair.of(this.id/4L,0L)).beginTimestamp = System.nanoTime();
        rolePlugin.episodeLeaderMode.put(0, Config.string("protocol.general.leader").equals("stable") ? 0 : 1);

        if (!isClient()) {
            // start the grpc server
            entityCommServer = new EntityCommServer(this);
            entityCommServer.start();

            // create the stub for learning agent
            ManagedChannel channel = Grpc
                    .newChannelBuilder("localhost:" + entityCommServer.agentPort, InsecureChannelCredentials.create())
                    .build();
            agentStub = AgentCommGrpc.newBlockingStub(channel);
        }

        systemStartTime = System.nanoTime();
    }

    private long lastSlowProposalTimestamp = 0;
    private List<RequestData> slowProposalRequests = new ArrayList<>();

    public void addSlowProposal(RequestData request) {
        synchronized (slowProposalRequests) {
            slowProposalRequests.add(request);
            slowProposalRequests.notify();
        }
    }

    public void triggerSlowProposal() {
        while (running) {
            var duration = slowProposalFault.getPerRequestDelay(this.id);

            synchronized (slowProposalRequests) {
                // wake up when met the block size
                while (slowProposalRequests.size() == 0 || 
                        (pendingRequests.size() + slowProposalRequests.size() < blockSize && running)) {
                    try {
                        slowProposalRequests.wait();
                    } catch (InterruptedException e) {
                    }
                }
            }

            // check if delay is enough
            var now = System.currentTimeMillis();
            if (now - lastSlowProposalTimestamp < duration) {
                try {
                    Thread.sleep(duration - (now - lastSlowProposalTimestamp));
                } catch (InterruptedException e) {
                }
            }
            lastSlowProposalTimestamp = System.currentTimeMillis();

            synchronized (slowProposalRequests) { 
                synchronized (pendingLock) {
                    // Fix:
                    // Notice that slow proposal may be triggered back and forth
                    // because prime `overwrites` the slow proposal attack
                    // here see if slowProposalRequests are removed
                    if (pendingRequests.size() + slowProposalRequests.size() < blockSize) {
                        continue;
                    }
                    var num_req = blockSize - pendingRequests.size();
                    for (int i = 0; i < num_req; i++) {
                        pendingRequests.offer(slowProposalRequests.remove(0));
                    }
                }
                // Printer.print(Verbosity.V, prefix, "[time-since-start=" + Printer.timeFormat(System.nanoTime() - systemStartTime, true) + "] packing slow proposal requests, ready for stateUpdate: nextSequence=" + nextSequence);
                System.out.println("For some reason I'm here 6 and nextSequence: " + nextSequence);
                stateUpdateLoop(Pair.of(this.id/4L, nextSequence));
            }
        }
    }

    private TreeSet<Long> aggregationBuffer = new TreeSet<>();
    private long lastLocalSeq = -1L;
    public void aggStateUpdate() {
        var aggregationDelay = Config.integer("benchmark.aggregation-delay-ms");
        while (running) {
            try {
                Thread.sleep(aggregationDelay);
            } catch (InterruptedException e) {
            }

            Long seqnum;
            synchronized (aggregationBuffer) {
                var newLocalSeq = new TreeSet<Long>();
                var highers = aggregationBuffer.tailSet(lastLocalSeq, false).iterator();

                // get consecutive seqnum from the aggregation buffer starting from lastLocalSeq+1
                while (highers.hasNext() && highers.next() == lastLocalSeq + 1) {
                    newLocalSeq.add(lastLocalSeq + 1);
                    lastLocalSeq += 1;
                }
                if (newLocalSeq.isEmpty()) {
                    continue;
                }

                // multiplexing global seqnum with local seqnum
                seqnum = newLocalSeq.first();
                var checkpoint = checkpointManager.getCheckpointForSeq(this.id/4L, seqnum);
                checkpoint.addAggregationValue(seqnum, newLocalSeq);
                // System.out.println("trigger aggStateUpdate seqnum: " + seqnum + ", newLocalSeq: " + newLocalSeq.toString());

                aggregationBuffer.clear();
            }
            System.out.println("Calling state update from aggStateupdate for seqnum: " + seqnum);
            stateUpdate(Pair.of(this.id/4L, seqnum));
        }
    }

    public void handleMessage(MessageData message) {
        System.out.println("Received request: " + message);
        if (Printer.verbosity >= Verbosity.VVV) {
            Printer.print(Verbosity.VVV, prefix, "Processing ", message);
        }
        System.out.println("Going to process: " + message);
        for (var i = messagePlugins.size() - 1; i >= 0; i--) {
            var plugin = messagePlugins.get(i);
            message = plugin.processIncomingMessage(message);
        }
        System.out.println("Processed: " + message);
        if (message.getFlagsList().contains(DataUtils.INVALID)) {
            System.out.println("Returning as message is invalid");
            return;
        }

        var type = message.getMessageType();
        if (type == StateMachine.REQUEST) {
            System.out.println("Received request: " + message);
            var request = message.getRequestsList().get(0);
            var seqnum = getRequestSequence(request.getRequestNum());
            if (seqnum == null) {
                // slow proposal
                System.out.println("Inside seqnum == null");
                if (slowProposalFault.getPerRequestDelay(this.id) > 0) {
                    System.out.println("Inside slowProposalFault.getPerRequestDelay(this.id) > 0");
                    // Printer.print(Verbosity.V, prefix, "[time-since-start=" + Printer.timeFormat(System.nanoTime() - systemStartTime, true) + "] add slow proposal: reqnum=" + request.getRequestNum());
                    addSlowProposal(request);
                } else {
                    // clean up if switch back to fault free
                    if (slowProposalRequests.size() > 0) {
                        System.out.println("Inside slowProposalRequests.size() > 0");
                        synchronized (slowProposalRequests) {
                            while (slowProposalRequests.size() > 0) {
                                pendingRequests.offer(slowProposalRequests.remove(0));
                            }
                            System.out.println("Inside slowProposalRequests.size() > 0: pendingRequests: " + pendingRequests);
                        }
                    }
                    // Printer.print(Verbosity.V, prefix, "[time-since-start=" + Printer.timeFormat(System.nanoTime() - systemStartTime, true) + "] received request: reqnum=" + request.getRequestNum());
                    System.out.println("Inserting into pending requests");
                    pendingRequests.offer(request);
                    System.out.println("Pending requests size: " + pendingRequests.size());
                    System.out.println("callling state update for next sequence number: " + nextSequence);
                    System.out.println("For some reason I'm here 4 and nextSequence: " + nextSequence);
                    stateUpdateLoop(Pair.of(this.id/4L, nextSequence));
                }
            }
        } else {
            Long seqnum = message.getSequenceNum();
            List<Integer> targets = message.getTargetsList();
            System.out.println("Received message inside not request block: " + message);
            System.out.println("My id is and Message Target is : " + id + " " + targets);
            if (checkpointManager.getCheckpointNum(this.id/4L, seqnum).getValue() < checkpointManager.getMinCheckpoint()) {
                return;
            }

            if (!isValidMessage(message)) {
                return;
            }
            System.out.println("Valid message: " + message);
            var checkpoint = checkpointManager.getCheckpointForSeq(this.id/4L, seqnum);
            checkpoint.tally(message);
            checkpoint.addAggregationValue(message);
            timekeeper.messageReceived(seqnum, currentViewNum, checkpoint.getState(Pair.of(this.id/4L, seqnum)), message);

            if (Printer.verbosity >= Verbosity.VVV) {
                Printer.print(Verbosity.VVV, prefix, "Tally message ", message);
            }
            System.out.println("Running state update loop for seqnum: " + seqnum); 
            System.out.println("For some reason I'm here 3 and seqnum: " + seqnum);
            stateUpdateLoop(Pair.of(this.id/4L, seqnum));
        }

        var start = DataUtils.toLong(message.getTimestamp());
        benchmarkManager.messageProcessed(start, System.nanoTime());
    }

    public void stateUpdateLoop(Pair<Long, Long> pair_clusternum_seqnum) {
        //Printer.print(Verbosity.VVVV, prefix, "StateUpdateLoop seqnum: " + seqnum);
        //System.out.println("Calling state update from staeUpdateLoop");
        var result = stateUpdate(pair_clusternum_seqnum);
        while (running && result != null && !result.isEmpty()) {
            var next = result.pollFirst();
            // System.out.println("This is causing issue for seqnum: " + next);
            var more = stateUpdate(next);
            if (more != null) {
                result.addAll(more);
            }
        }
    }

    public TreeSet<Pair<Long, Long>> stateUpdate(Pair<Long, Long> pair_clusternum_seqnum) {
        // Printer.print(Verbosity.V, prefix, "[time-since-start=" + Printer.timeFormat(System.nanoTime() - systemStartTime, true) + "] begin stateUpdate: seqnum=" + seqnum + 
        //             " (nextSequence=" + nextSequence + ", lastExecutedSequenceNum=" + lastExecutedSequenceNum + ")");
        
        //System.out.println("Inside state update for seqnum: " + seqnum);
        benchmarkManager.add(BenchmarkManager.STATE_UPDATE, 0, System.nanoTime());

        // Do not process messages belonging to the next episode
        if (pair_clusternum_seqnum.getValue() > getEndOfEpisode()) {
            System.out.println("Returning null as seqnum > getEndOfEpisode()");
            return null;
        }

        stateLock.lock();

        // TODO: concurrency control for leader rotation protocols
        if (pair_clusternum_seqnum.getValue() <= lastExecutedSequenceNum || isExecuted(pair_clusternum_seqnum.getValue())
                || (isPrimary(pair_clusternum_seqnum.getValue() ) && pair_clusternum_seqnum.getValue() - lastExecutedSequenceNum > pipelinePlugin.getMaxActiveSequences())) {
            stateLock.unlock();
            //System.out.println("Returning null as seqnum <= lastExecutedSequenceNum and isExecuted(seqnum) = " + isExecuted(seqnum) + "\n and isPrimary(seqnum) = " + isPrimary(seqnum) + "lastExecutedSequenceNum = " + lastExecutedSequenceNum);
            return null;
        }
        benchmarkManager.add(BenchmarkManager.IF1, 0, System.nanoTime());
        if (pair_clusternum_seqnum.getValue() > nextSequence) {
            System.out.println("Returning null as seqnum > nextSequence");
            needsUpdate.add(pair_clusternum_seqnum);
            stateLock.unlock();
            return null;
        }
        benchmarkManager.add(BenchmarkManager.IF2, 0, System.nanoTime());

        if (updating.contains(pair_clusternum_seqnum)) {
            needsUpdate.add(pair_clusternum_seqnum);
            stateLock.unlock();
            System.out.println("Returning null as updating.contains(seqnum)");
            return null;
        }
        benchmarkManager.add(BenchmarkManager.IF3, 0, System.nanoTime());

        //System.out.println("Adding seqnum to updating: " + seqnum);
        updating.add(pair_clusternum_seqnum);
        stateLock.unlock();

        var stateUpdated = false;
        var nextseqUpdated = false;
        var seqExecuted = false;

        int local_cnt = 0;
        benchmarkManager.add(BenchmarkManager.BEGIN_WHILE_LOOP, 0, System.nanoTime());
        while (running) {
            local_cnt++;
            System.out.println("Inside while loop");
            stateUpdated = false;

            var checkpoint = checkpointManager.getCheckpointForSeq(this.id/4L, pair_clusternum_seqnum.getValue());
            var currentState = checkpoint.getState(Pair.of(this.id/4L, pair_clusternum_seqnum.getValue()));
            var phase = StateMachine.states.get(currentState).phase;
            var roles = rolePlugin.getEntityRoles(pair_clusternum_seqnum.getValue(), currentViewNum, phase, id);
            var roles2 = rolePlugin.getRoleEntities(pair_clusternum_seqnum.getValue(), pair_clusternum_seqnum.getValue(), local_cnt, local_cnt, local_cnt);
        // Print roles
            System.out.println(" Printing roles: ");
            for (var role : roles) {
                System.out.println("Role: " + role);
            }
            System.out.println("Inside pending requests1");
            // System.out.println("[" + String.join(",", roles.stream().map(x -> x + "").toList()) + "]");

            // System.out.println(prefix + "seq_num: " + seqnum + "\t local_cnt: " + local_cnt + "\t current state: " + StateMachine.states.get(currentState).name);

            searchloop:
            // System.out.println("Inside pending requests2");
            for (var statenum : List.of(currentState, StateMachine.ANY_STATE)) {
                if (statenum == -1) {
                    continue;
                }
                System.out.println("Inside pending requests3");
                var state = StateMachine.states.get(statenum);
                System.out.println("State: " + state.name);
                for (var role : roles) {
                    var candidates = state.transitions.get(role);
                    if (candidates == null) {
                        System.out.println("Candidate is null");
                        continue;
                    }
                    //Printing all the candidates
                    for (var transition : candidates) {
                        System.out.println("Transition: " + transition);
                    }
                    for (var transition : candidates) {
                        System.out.println("Inside pending requests4");
                        var condition = transition.condition;
                        var conditionType = condition.getType();

                        var conditionMet = false;
                        System.out.println("Condition type: " + conditionType);
                        if (conditionType == Condition.TRUE_CONDITION) {
                            System.out.println("Inside condition type true");
                            conditionMet = true;
                        } else if (conditionType == Condition.MESSAGE_CONDITION) {
                            var messageType = condition.getParam(Condition.MESSAGE_TYPE);
                            System.out.println("The message type is: " + messageType);
                            if (messageType == StateMachine.REQUEST) {                                                 
                                var block = checkpoint.getRequestBlock(pair_clusternum_seqnum);
                                System.out.println("Inside pending requests5");
                                System.out.println("Pending request size and block size: " + pendingRequests.size() + " " + blockSize);
                                if (block == null || block.isEmpty()) {
                                    synchronized (pendingLock) {
                                        if (pendingRequests.size() < blockSize) {
                                            System.out.println("Inside pending requests6 and pending request size: " + pendingRequests.size());
                                            //continue;
                                        }

                                        // seqnum reserved for feature exchange
                                        if (learning && pair_clusternum_seqnum.getValue() == exchangeSequence && isPrimary(pair_clusternum_seqnum.getValue())) {
                                            if (!reportTally.hasQuorum(Pair.of(this.id/4L, Long.valueOf(currentEpisodeNum.get())), 0, new QuorumId(REPORT, REPORT_QUORUM))) {
                                                System.out.println("Inside pending requests7 and report tally has no quorum");
                                                System.out.println("Breaking searchloop2");
                                                break searchloop;
                                            }
                                        }

                                        block = new ArrayList<RequestData>(blockSize);
                                        
                                        // for (var i = 0; i < blockSize; i++) {
                                            var request = pendingRequests.size() == 0 ? null : pendingRequests.remove();
                                            if (request == null) {
                                                continue;
                                            } 
                                            // carry the report quorum in the first request of this reserved block
                                            // if (learning && seqnum == exchangeSequence && isPrimary(seqnum) && i == 0) 
                                            if (learning && pair_clusternum_seqnum.getValue() == exchangeSequence && isPrimary(pair_clusternum_seqnum.getValue())) {
                                                var reportQuorum = new ArrayList<LearningData>(REPORT_QUORUM);
                                                reports.get(currentEpisodeNum.get()).entrySet().stream().limit(REPORT_QUORUM).forEach(entry -> {
                                                    var learningDataBuilder = LearningData.newBuilder().putAllReport(entry.getValue());
                                                    reportQuorum.add(learningDataBuilder.build());
                                                });
                                                request = request.toBuilder().addAllReportQuorum(reportQuorum).build();                                                
                                            }
                                            block.add(request);
                                        //}
                                    }
                                }

                                var message = createMessage(pair_clusternum_seqnum.getValue(), currentViewNum, block, StateMachine.REQUEST, id,
                                        List.of(id));
                                System.out.println("Inside pending requests8 and message: ");
                                checkpoint.tally(message);
                                benchmarkManager.add(BenchmarkManager.CREATE_REQUEST_BLOCK, 0, System.nanoTime());
                                
                                proposedRequests += 1;
                                // Printer.print(Verbosity.V, prefix, "Create proposal, seqnum: " + seqnum);
                            }

                            var quorumId = new QuorumId(messageType, condition.getParam(Condition.QUORUM));
                            conditionMet = checkMessageTally(pair_clusternum_seqnum.getValue(), quorumId, transition.updateMode);
                            System.out.println("Inside pending requests8 + conditionMet: " + conditionMet);
                        }

                        if (conditionMet) {
                            benchmarkManager.add(BenchmarkManager.CONDITION_MET, 0, System.nanoTime());

                            timekeeper.stateUpdated(pair_clusternum_seqnum.getValue(), transition.toState);

                            if (StateMachine.isIdle(currentState)) {
                                benchmarkManager.sequenceStarted(pair_clusternum_seqnum.getValue(), System.nanoTime());
                            }

                            if (transition.updateMode == UpdateMode.AGGREGATION && checkpoint.getAggregationValues(pair_clusternum_seqnum.getValue()).isEmpty()) {
                                // store local seqnum (multiplexing with global seqnum) in the aggregation buffer
                                synchronized (aggregationBuffer) {
                                    aggregationBuffer.add(pair_clusternum_seqnum.getValue());
                                }
                                System.out.println("Breaking searchloop1");
                                break searchloop;
                            }

                            // track slow path ratio for sbft
                            if (transition.updateMode == UpdateMode.SLOW) {
                                featureManager.countPath(currentEpisodeNum.get(), FeatureManager.FAST_PATH_FREQUENCY, FeatureManager.SLOW);    
                            }

                            transition(pair_clusternum_seqnum.getValue(), transition);
                            stateUpdated = true;
                            // Printer.print(Verbosity.V, prefix, "[time-since-start=" + Printer.timeFormat(System.nanoTime() - systemStartTime, true) + "] transition state to: seqnum=" + seqnum + 
                            //                 ", toState=" + StateMachine.states.get(transition.toState).name);

                            stateLock.lock();
                            if (nextSequence == pair_clusternum_seqnum.getValue()) {
                                System.out.println("Increasing next sequence by 1 "+ nextSequence);
                                nextSequence += 1;
                                nextseqUpdated = true;
                                // Printer.print(Verbosity.V, prefix, "[time-since-start=" + Printer.timeFormat(System.nanoTime() - systemStartTime, true) + "] update nextSequence to: seqnum=" + seqnum + 
                                //             ", nextSequence=" + nextSequence);
                            }

                            if (transition.updateMode == UpdateMode.SEQUENCE) {
                                seqExecuted = true;
                                // Printer.print(Verbosity.V, prefix, "[time-since-start=" + Printer.timeFormat(System.nanoTime() - systemStartTime, true) + "] ready for execution: seqnum=" + seqnum);
                            }
                            stateLock.unlock();
                            System.out.println("Breaking searchloop3");
                            break searchloop;
                        }
                    }
                }
            }

            if (!stateUpdated) {
                var overdue = timekeeper.getOverdue(pair_clusternum_seqnum.getValue());
                if (overdue != null) {
                    // System.out.println(prefix + "seq_num: " + seqnum + "\t overdue, dueLength = " + overdue.dueLength / 1000.0 + "us");
                    var transition = overdue.transition;
                    if (transition != null) {
                        benchmarkManager.start(BenchmarkManager.TIMEOUT, pair_clusternum_seqnum.getValue(), 1);

                        transition(pair_clusternum_seqnum.getValue(), transition);
                        timekeeper.stateUpdated(pair_clusternum_seqnum.getValue(), transition.toState);

                        stateUpdated = true;
                    }
                }
            }

            if (seqExecuted || !stateUpdated) {
                System.out.println("Breaking while loop with seqExecuted: " + seqExecuted + " stateUpdated: " + stateUpdated);
                break;
            }
        }

        stateLock.lock();
        updating.remove(pair_clusternum_seqnum);
        var toUpdate = new TreeSet<Pair<Long,Long>>();
        if (nextseqUpdated || needsUpdate.contains(Pair.of(this.id/4, nextSequence))) {
            //System.out.println("Adding nextSequence to toUpdate1: " + nextSequence);
            //System.out.println("nextseqUpdated: " + nextseqUpdated + " need.update.contains(nextSequence): " + needsUpdate.contains(nextSequence));
            toUpdate.add(Pair.of(this.id/4L, nextSequence));
        }

        if (needsUpdate.contains(pair_clusternum_seqnum)) {
            needsUpdate.remove(pair_clusternum_seqnum);
            //System.out.println("Adding seqnum to toUpdate2: " + seqnum);
            toUpdate.add(pair_clusternum_seqnum);
        }
        stateLock.unlock();

        return toUpdate;
    }

    public Transition processTransition(long seqnum, int state, Transition transition) {
        for (var i = 0; i < transitionPlugins.size(); i++) {
            var plugin = transitionPlugins.get(i);
            transition = plugin.processTransition(seqnum, state, transition);
            if (transition == null) {
                break;
            }
        }

        return transition;
    }

    public void executor() {
        while (running) {
            Transition transition;
            var clusterExecutionQueue = executionQueue.get(getId()/4L);
            synchronized (clusterExecutionQueue) {
                while (clusterExecutionQueue.get(lastExecutedSequenceNum + 1) == null && running) {
                    try {
                        clusterExecutionQueue.wait();
                    } catch (InterruptedException e) {
                    }
                }

                transition = clusterExecutionQueue.get(lastExecutedSequenceNum + 1);
                clusterExecutionQueue.entrySet().removeIf(entry -> entry.getKey() <= lastExecutedSequenceNum + 1);
                lastExecutedSequenceNum += 1;

                execute(lastExecutedSequenceNum);
                // Printer.print(Verbosity.V, prefix, "[time-since-start=" + Printer.timeFormat(System.nanoTime() - systemStartTime, true) + "] executed by executor thread: seqnum=" + lastExecutedSequenceNum);
            }

            var checkpoint = checkpointManager.getCheckpointForSeq(this.id/4L, lastExecutedSequenceNum);
            var localSeqs = checkpoint.getAggregationValues(lastExecutedSequenceNum);

            // if aggregation then perform execution for all local seq
            if (!localSeqs.isEmpty() && !isClient()) {
                // System.out.println("begin execution for global order: " + lastExecutedSequenceNum);
                for (var localSeq : localSeqs) {
                    // System.out.println("executing localSeq: " + localSeq);
                    if (localSeq != lastExecutedSequenceNum) {
                        // avoid getting null blocks
                        while (checkpoint.getRequestBlock(Pair.of(this.id/4L,localSeq)) == null) {
                            try {
                                Thread.sleep(1);
                            } catch (InterruptedException e) {
                            }
                        }
                        execute(localSeq);
                    }

                    benchmarkManager.sequenceExecuted(localSeq, System.nanoTime());
                    checkpoint.setState(Pair.of(this.id/4L,localSeq), transition.toState);

                    checkSwitching(localSeq);
                    transition(localSeq, transition);
                }
                lastExecutedSequenceNum = localSeqs.pollLast();
                // System.out.println("lastExecutedSequenceNum update to: " + lastExecutedSequenceNum);
            } else {
                benchmarkManager.sequenceExecuted(lastExecutedSequenceNum, System.nanoTime());
                checkpoint.setState(Pair.of(this.id/4L, lastExecutedSequenceNum), transition.toState);

                checkSwitching(lastExecutedSequenceNum);
                transition(lastExecutedSequenceNum, transition);
            }
            // checkSwitching(lastExecutedSequenceNum);

            //Commeting this part out because causing issue with seqnum
            System.out.println("For some reason I'm here 2 and running for lastExecutedSequenceNum+1: " + lastExecutedSequenceNum);
            stateUpdateLoop(Pair.of(this.id/4L, lastExecutedSequenceNum + 1));
        }
    }

    private void checkSwitching(long seqnum) {
        if (protocols.isEmpty() && !learning) {
            return;
        }

        var checkpoint = checkpointManager.getCheckpointForSeq(this.id/4L, seqnum);

        // Switch to the next episode
        if (seqnum == getEndOfEpisode(seqnum)) {
            var episodeDuration = (System.nanoTime() - checkpoint.beginTimestamp) / 1e9f;
            cumulativeDuration += (double) episodeDuration;
            var throughput = benchmarkManager.getBenchmarkByEpisode(currentEpisodeNum.get())
                    .count(BenchmarkManager.REQUEST_EXECUTE) / episodeDuration;

            var episodeReport = "[EPISODE REPORT] episode " + currentEpisodeNum.get() + ": protocol = " + checkpoint.getProtocol()
                    + " , throughput = " + String.format("%.2freq/s", throughput) + " , episode time = " + episodeDuration + "s, overall time = " + cumulativeDuration + "s";
            System.out.println(episodeReport);
            Printer.print(Verbosity.V, prefix, episodeReport);
            Printer.flush();
            checkpoint.throughput = throughput;

            String nextProtocol;
            if (!isClient() && !protocols.isEmpty()) {
                // static switching in debug mode
                nextProtocol = protocols.get(currentEpisodeNum.get() % protocols.size());
            } else {
                // dynamic switching via learning agent
                // or client
                nextProtocol = checkpoint.getDecision();
            }
            // warm up episodes
            if (nextProtocol.equals("repeat")) {
                nextProtocol = checkpoint.getProtocol();
            }
            System.out.println(prefix + "nextProtocol = " + nextProtocol); 
            Printer.print(Verbosity.V, prefix, "nextProtocol = " + nextProtocol);
            Printer.flush();

            var checkpointNew = checkpointManager.getCheckpointForSeq(this.id/4L, seqnum + 1);
            checkpointNew.setProtocol(nextProtocol);
            
            // Record start of the next episode
            checkpointNew.beginTimestamp = System.nanoTime();

            // Reload special knobs in protocol.config file
            slowProposalFault.reloadProtocol(nextProtocol);
            indarkFault.reloadProtocol(nextProtocol);
            
            // Update localSeq for Prime
            if (nextProtocol.equals("prime")) {
                synchronized (aggregationBuffer) {
                    lastLocalSeq = seqnum;
                }
            }

            // Update epoch to leader mode mapping
            Config.setCurrentProtocol(nextProtocol);
            rolePlugin.roleWriteLock.lock();
            try {
                rolePlugin.episodeLeaderMode.put(currentEpisodeNum.get() + 1, 
                        Config.string("protocol.general.leader").equals("stable") ? 0 : 1);
                System.out.println("leader mode set to be " + Config.string("protocol.general.leader") + " for the next episode");
                Printer.print(Verbosity.V, prefix, "leader mode set to be " + Config.string("protocol.general.leader") + " for the next episode");
                Printer.flush();
                // signal that leader mode for a new episode is available
                rolePlugin.roleCondition.signalAll();
            } finally {
                rolePlugin.roleWriteLock.unlock();
            }
        
            // Update report and exchange sequence
            reportSequence += EPISODE_SIZE;
            exchangeSequence += EPISODE_SIZE;

            currentEpisodeNum.incrementAndGet();
        }
    }

    public void setServiceState(Map<Integer, Integer> service_state, long lastExecutedSequenceNum) {
        // lock on execution
        var clusterExecutionQueue = executionQueue.get(getId()/4L);
        synchronized (clusterExecutionQueue) {
            dataset.setRecords(service_state);
            
            stateLock.lock();
            this.lastExecutedSequenceNum = lastExecutedSequenceNum;
            this.nextSequence = lastExecutedSequenceNum + 1;
            stateLock.unlock();
            System.out.println("For some reason I'm here 1 and running for lastExecutedSequenceNum+1: " + lastExecutedSequenceNum);
            new Thread(() -> stateUpdateLoop(Pair.of(this.id/4L,lastExecutedSequenceNum + 1))).start();
        }
    }

    public boolean transition(long seqnum, Transition transition) {
        benchmarkManager.add(BenchmarkManager.TRANSITION, 0, System.nanoTime());
        var checkpoint = checkpointManager.getCheckpointForSeq(this.id/4L,seqnum);
        var currentState = checkpoint.getState(Pair.of(this.id/4L,seqnum));
        var phase = StateMachine.states.get(currentState).phase;

        var requestBlock = checkpoint.getRequestBlock(Pair.of(this.id/4L,seqnum));

        // if update mode is sequence mode
        // **and** current state is not the same as the target state
        // this shall be the execution transition
        if (transition.updateMode == UpdateMode.SEQUENCE && currentState != transition.toState) {
            // special case for proposal slowness measurement on zyzzyva leader 
            if (checkpoint.getProtocol().equals("zyzzyva") && isPrimary(seqnum)) {
                featureManager.received(getEpisodeNum(seqnum), seqnum);
            }

            Printer.print(Verbosity.VVV, prefix, "Execution START: " + seqnum);
            // execute(seqnum);
            executionQueue.putIfAbsent(getId()/4L, new HashMap<>());
            var clusterExecutionQueue = executionQueue.get(getId()/4L);
            synchronized (clusterExecutionQueue) {
                clusterExecutionQueue.put(seqnum, transition);
                clusterExecutionQueue.notify();
            }
            return false;
        }

        checkpoint.setState(Pair.of(this.id/4L,seqnum), transition.toState);
        if (transition.updateMode == UpdateMode.VIEW) {
            pendingRequests.clear();
            currentViewNum += 1;
        }

        var messages = new ArrayList<MessageData>();

        for (var pair : transition.responses) {
            /*
             * This is written to incorporate the case such that
             * in case a transition needs some response messages to be sent
             * it adds to messages list and creates target list accordingly
             */
            var role = pair.getLeft();
            var messageType = pair.getRight();
            System.out.println("Currently at Role: " + role + " MessageType: " + messageType);
            if (role == StateMachine.CLIENT) {
                var targets = List.copyOf(requestBlock.stream().map(r -> r.getClient()).collect(Collectors.toSet()));
                var message = createMessage(seqnum, currentViewNum, null, messageType, id+(4*(this.coordinator.getClusterNum()-1)), List.of(4));
                System.out.println("Inside client response and targets: " + targets);
                messages.add(message);
            } else {
                var targets = rolePlugin.getRoleEntities(seqnum, currentViewNum, phase, role, this.getCoordinator().getClusterNum());
                var message = createMessage(seqnum, currentViewNum, null, messageType, id, targets);
                messages.add(message);
                System.out.println("Current targets: " + targets);
            }
        }

        for (var pair : transition.extraTally) {
            var role = pair.getLeft();
            var messageType = pair.getRight();
            var entities = rolePlugin.getRoleEntities(seqnum, currentViewNum, phase, role, this.getCoordinator().getClusterNum());

            // Message from entities to self
            // if entities = [primary]
            // then the message will be [0 -> [0], 0 -> [1], 0 -> [2], 0 -> [3],]
            for (var entity : entities) {
                var message = createMessage(seqnum, currentViewNum, null, messageType, entity, List.of(id));
                checkpoint.tally(message);

                if (Printer.verbosity >= Verbosity.VVV) {
                    Printer.print(Verbosity.VVV, prefix, "Processing extra ", message);
                    Printer.print(Verbosity.VVV, prefix, "Tally message extra ", message);
                }
            }
        }

        messages.parallelStream().forEach(m -> sendMessage(m));
        for (var message : messages) {
            if (message.getTargetsList().contains(id)) {
                checkpoint.tally(message);

                if (Printer.verbosity >= Verbosity.VVV) {
                    Printer.print(Verbosity.VVV, prefix, "Processing self via transition ", message);
                    Printer.print(Verbosity.VVV, prefix, "Tally message self via transition ", message);
                }
            }
        }

        var roles = rolePlugin.getEntityRoles(seqnum, currentViewNum, phase, id);
        for (var statenum : List.of(transition.toState, StateMachine.ANY_STATE)) {
            if (statenum < 0) {
                continue;
            }
            var state = StateMachine.states.get(statenum);
            for (var role : roles) {
                var candidates = state.transitions.get(role);
                if (candidates == null) {
                    continue;
                }

                for (var candidate : candidates) {
                    var condition = candidate.condition;
                    if (condition.getType() == Condition.TIMEOUT_CONDITION) {
                        var mode = condition.getParam(Condition.TIMEOUT_MODE);
                        var multiplier = condition.getParam(Condition.TIMEOUT_MULTIPLIER);
                        timekeeper.startTimer(seqnum, currentViewNum, statenum, mode, multiplier, candidate);
                    }
                }
            }
        }

        if (Printer.verbosity >= Verbosity.VVV) {
            var prevstr = StateMachine.states.get(currentState).name;
            var nextstr = StateMachine.states.get(transition.toState).name;
            var debugstr = new StringBuilder("Transition ").append(seqnum).append(" from ")
                                                           .append(prevstr).append(" to ")
                                                           .append(nextstr).toString();
            Printer.print(Verbosity.VVV, prefix, debugstr);
        }

        for (var i = 0; i < transitionPlugins.size(); i++) {
            var plugin = transitionPlugins.get(i);
            /*
             * Not doing anything
             */
            plugin.postTransition(seqnum, currentState, transition);
        }

        return true;
    }

    public boolean isPrimary() {
        var is_primary = false;
        if (rolePlugin instanceof BasicPrimaryPlugin) {
            // judge whether is primary in the first epoch (this is only used for generating leader attack)
            if (rolePlugin.getEntityRoles(0, currentViewNum, 0, this.id).contains(StateMachine.roles.indexOf("primary"))) {
                is_primary = true;
            }
        } else if (rolePlugin instanceof PrimaryPassivePlugin) {
            if (rolePlugin.getEntityRoles(0, currentViewNum, StateMachine.NORMAL_PHASE, this.id).contains(StateMachine.roles.indexOf("primary"))) {
                is_primary = true;
            }
        } else {
            // TODO: handle other cases
        }
        return is_primary;
    }

    public boolean isPrimary(long seqnum) {
        var is_primary = false;
        if (rolePlugin instanceof BasicPrimaryPlugin) {
            // judge whether is primary
            if (rolePlugin.getEntityRoles(seqnum, currentViewNum, 0, this.id).contains(StateMachine.roles.indexOf("primary"))) {
                is_primary = true;
            }
        } else if (rolePlugin instanceof PrimaryPassivePlugin) {
            if (rolePlugin.getEntityRoles(seqnum, currentViewNum, StateMachine.NORMAL_PHASE, this.id).contains(StateMachine.roles.indexOf("primary"))) {
                is_primary = true;
            }
        } else {
            // TODO: handle other cases
        }
        return is_primary;
    }

    public void sendMessage(MessageData message) {
        System.out.println("Sending message: " + message);
        if (message.getFlagsList().contains(DataUtils.INVALID)) {
            return;
        }

        if (Printer.verbosity >= Verbosity.VVV) {
            Printer.print(Verbosity.VVV, prefix, "Sending message: ", message);
        }
        // Printer.print(Verbosity.V, prefix, "Sending message: ", message);

        pipelinePlugin.sendMessage(message, this.id);
    }

    public MessageData createMessage(Long seqnum, long viewNum, List<RequestData> block, int type, int source,
            List<Integer> targets) {

        ByteString digest = null;
        Map<Long, Integer> replies = null;
        MessageData message;
        Set<Long> aggregationValues = null;

        if (seqnum != null) {
            var checkpoint = checkpointManager.getCheckpointForSeq(this.id/4L,seqnum);

            if (block == null) {
                block = checkpoint.getRequestBlock(Pair.of(this.id/4L,seqnum));
            }

            digest = checkpoint.getMessageTally().getQuorumDigest(Pair.of(this.id/4L,seqnum), viewNum);
            if (digest == null) {
                digest = DataUtils.getDigest(block);
            }

            if (type == StateMachine.REPLY) {
                replies = checkpoint.getReplies(Pair.of(this.id/4L,seqnum));
            }

            if (!checkpoint.getAggregationValues(seqnum).isEmpty()) {
                aggregationValues = checkpoint.getAggregationValues(seqnum);
            }
        }

        var hasblock = StateMachine.messages.get(type).hasRequestBlock;
        System.out.println("My source is: " + source);
        System.out.println("My messsage type is: " + type);
        System.out.println("My targets are: " + targets);
        if (hasblock) {
            message = DataUtils.createMessage(seqnum, viewNum, type, source, targets, null, block, replies, digest);
        } else {
            var reqnums = block.stream().map(req -> req.getRequestNum()).toList();
            message = DataUtils.createMessage(seqnum, viewNum, type, source, targets, reqnums, null, replies, digest);
        }

        // carry aggregation values if exist
        if (aggregationValues != null) {
            message = message.toBuilder().addAllAggregationValues(aggregationValues).build();
        }

        // notify client about the next protocol inside REPLY message
        if (seqnum != null && seqnum == getEndOfEpisode(seqnum) && type == StateMachine.REPLY) {
            var checkpointNew = checkpointManager.getCheckpointForSeq(this.id/4L,seqnum + 1);
            var protocol = checkpointNew.getProtocol();

            var switchingDataBuilder = SwitchingData.newBuilder().setNextProtocol(protocol);
            message = message.toBuilder().setSwitch(switchingDataBuilder).build();        
            // System.out.println("createMessage: attach nextProtocol = " + protocol + " to REPLY message");  
        }

        return processMessage(message);
    }

    public MessageData processMessage(MessageData message) {
        for (var i = 0; i < messagePlugins.size(); i++) {
            var plugin = messagePlugins.get(i);
            message = plugin.processOutgoingMessage(message);
        }

        return message;
    }

    protected boolean checkMessageTally(long seqnum, QuorumId quorumId, UpdateMode updateMode) {
        var checkpoint = checkpointManager.getCheckpointForSeq(this.id/4L,seqnum);
        var tally = checkpoint.getMessageTally();
        var checkview = updateMode == UpdateMode.VIEW ? currentViewNum + 1 : currentViewNum;
        //var quorumId = new QuorumId(condition.getParam(Condition.MESSAGE_TYPE), condition.getParam(Condition.QUORUM));
        var viewnum = tally.getMaxQuorum(Pair.of(this.id/4L,seqnum), quorumId);
        System.out.println("Inside checkMessageTally: seqnum: " + seqnum + ", quorumId: " + quorumId + ", viewnum: " + viewnum + ", checkview: " + checkview);
        if (viewnum != null && viewnum == checkview) {
            var block = tally.getQuorumBlock(Pair.of(this.id/4L,seqnum), viewnum);
            if (block != null) {
                registerBlock(seqnum, block);
            }

            if (Printer.verbosity >= Verbosity.VVVVV) {
                Printer.print(Verbosity.VVVVV, "Checking Message Tally Success: ", StateMachine.messages.get(quorumId.message).name.toUpperCase() + " seqnum: " + seqnum + " size: " + quorumId.quorum);
            }
            return true;
        }

        if (Printer.verbosity >= Verbosity.VVVVV) {
            Printer.print(Verbosity.VVVVV, "Checking Message Tally Failed: ", StateMachine.messages.get(quorumId.message).name.toUpperCase() + " seqnum: " + seqnum + " size: " + quorumId.quorum);
        }

        return false;
    }

    public boolean isValidMessage(MessageData message) {
        // var type = message.getMessageType();
        // var messagePhases = StateMachine.messages.get(type).phases;

        // var seqnum = message.getSequenceNum();
        // var checkpoint = checkpointManager.getCheckpointForSeq(seqnum);
        // var statenum = checkpoint.getState(seqnum);
        // var currentPhase = StateMachine.states.get(statenum).phase;

        // return currentPhase == StateMachine.NORMAL_PHASE || messagePhases.contains(currentPhase);
        return true;
    }

    protected boolean isExecuted(long seqnum) {
        return checkpointManager.getCheckpointForSeq(this.id/4L,seqnum).getState(Pair.of(this.id/4L,seqnum)) == StateMachine.EXECUTED;
    }

    protected void registerBlock(long seqnum, List<RequestData> block) {
        var checkpoint = checkpointManager.getCheckpointForSeq(this.id/4L,seqnum);
        if (checkpoint.getRequestBlock(Pair.of(this.id/4L,seqnum)) == null) {
            for (var request : block) {
                reqnumToSeqnumMap.put(request.getRequestNum(), seqnum);
            }

            checkpoint.addRequestBlock(Pair.of(this.id/4L,seqnum), block);
            //// benchmarkManager.sequenceStarted(seqnum, DataUtils.timeus());
        }
    }

    public Long getRequestSequence(long reqnum) {
        return reqnumToSeqnumMap.get(reqnum);
    }

    public void start() {
        benchmarkManager.start();
        // TODO: Use Virtual Threads
        threads.forEach(thread -> thread.start());
    }

    public void stop() {
        running = false;
        threads.forEach(thread -> thread.interrupt());
    }

    public void registerThread(Thread thread) {
        threads.add(thread);
    }

    protected int reportnum = 0;

    public Map<String, String> reportBenchmark() {
        var benchmark = benchmarkManager.getBenchmarkById(reportnum);

        var report = new HashMap<String, String>();
        var queueMax = benchmark.max(BenchmarkManager.REQUEST_QUEUE);
        var queueAvg = benchmark.average(BenchmarkManager.REQUEST_QUEUE);
        report.put("request-queue",
                "avg: " + Printer.timeFormat(queueAvg, true) + ", max: " + Printer.timeFormat(queueMax, true));

        var messageMax = benchmark.max(BenchmarkManager.MESSAGE_PROCESS);
        var messageAvg = benchmark.average(BenchmarkManager.MESSAGE_PROCESS);
        var messageCount = benchmark.count(BenchmarkManager.MESSAGE_PROCESS);
        report.put("message-process",
                "avg: " + Printer.timeFormat(messageAvg, true) + ", max: " + Printer.timeFormat(messageMax, true) + ", count: "
                        + messageCount);

        var blockMax = benchmark.max(BenchmarkManager.BLOCK_EXECUTE);
        var blockAvg = benchmark.average(BenchmarkManager.BLOCK_EXECUTE);
        var blockCount = benchmark.count(BenchmarkManager.BLOCK_EXECUTE);
        report.put("block-execute",
                "avg: " + Printer.timeFormat(blockAvg, true) + ", max: " + Printer.timeFormat(blockMax, true) + ", count: "
                        + blockCount);

        // report.put("state-update", "count: " + benchmark.count(BenchmarkManager.STATE_UPDATE));
        // report.put("state-update-if1", "count: " + benchmark.count(BenchmarkManager.IF1));
        // report.put("state-update-if2", "count: " + benchmark.count(BenchmarkManager.IF2));
        // report.put("state-update-if3", "count: " + benchmark.count(BenchmarkManager.IF3));
        // report.put("begin-while-loop", "count: " + benchmark.count(BenchmarkManager.BEGIN_WHILE_LOOP));
        // report.put("create-request-block", "count: " + benchmark.count(BenchmarkManager.CREATE_REQUEST_BLOCK));
        // report.put("condition-met", "count: " + benchmark.count(BenchmarkManager.CONDITION_MET));
        // report.put("transition", "count: " + benchmark.count(BenchmarkManager.TRANSITION));
        report.put("last-executed-sequence", "num: " + lastExecutedSequenceNum);
        report.put("in-dark", "value: " + indarkFault.getApply());

        var timeoutCount = benchmark.count(BenchmarkManager.TIMEOUT);
        report.put("slow-path", String.format("ratio: %.2f",  (double) timeoutCount / (double) blockCount));
        report.put("proposal (since-start)", "count: " + proposedRequests);

        report.put("current-episode", "value: " + currentEpisodeNum.get());
        //report.put("current-protocol", "value: " + checkpointManager.getCheckpoint(Pair.of(this.id/4L,currentEpisodeNum.get())).getProtocol());

        reportnum += 1;
        return report;
    }

    // Abstract Methods

    protected abstract void execute(long seqnum);

    public abstract boolean isClient();

    // Getters

    public int getId() {
        return id;
    }

    public boolean isRunning() {
        return running;
    }

    public long getLastExecutedSequenceNum() {
        return lastExecutedSequenceNum;
    }

    public long getNextSequence() {
        return nextSequence;
    }

    public long getCurrentViewNum() {
        return currentViewNum;
    }

    public Timekeeper getTimekeeper() {
        return timekeeper;
    }

    public CoordinatorUnit getCoordinator() {
        return coordinator;
    }

    public CheckpointManager getCheckpointManager() {
        return checkpointManager;
    }

    public RolePlugin getRolePlugin() {
        return rolePlugin;
    }

    public List<MessagePlugin> getMessagePlugins() {
        return messagePlugins;
    }

    public List<TransitionPlugin> getTransitionPlugins() {
        return transitionPlugins;
    }

    public InDarkFault getInDarkFault() {
        return indarkFault;
    }

    public TimeoutFault getTimeoutFault() {
        return timeoutFault;
    }

    public SlowProposalFault getSlowProposalFault() {
        return slowProposalFault;
    }

    public PollutionFault getPollutionFault() {
        return pollutionFault;
    }

    public Map<Integer, Map<Integer, Map<Integer, Float>>> getReports() {
        return reports;
    }

    public MessageTally getReportTally() {
        return reportTally;
    }

    public FeatureManager getFeatureManager() {
        return featureManager;
    }

    public long getEndOfEpisode() {
        return (currentEpisodeNum.get() + 1) * EPISODE_SIZE - 1;
    }
    
    public long getEndOfEpisode(long seqnum) {
        return (getEpisodeNum(seqnum) + 1) * EPISODE_SIZE - 1;
    }

    public long getBeginOfEpisode(long seqnum) {
        return getEpisodeNum(seqnum) * EPISODE_SIZE;
    }

    public int getEpisodeNum(long seqnum) {
        return (int) (seqnum / EPISODE_SIZE);
    }
}
