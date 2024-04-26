package com.gbft.framework.coordination;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.tuple.Pair;

import com.gbft.framework.core.Client;
import com.gbft.framework.core.DynamicClient;
import com.gbft.framework.core.Entity;
import com.gbft.framework.core.Node;
import com.gbft.framework.core.ShardingClient;
import com.gbft.framework.data.Event;
import com.gbft.framework.data.FaultData;
import com.gbft.framework.data.Event.EventType;
import com.gbft.framework.data.MessageData;
import com.gbft.framework.data.ReportData;
import com.gbft.framework.data.ReportData.ReportItem;
import com.gbft.framework.data.UnitData;
import com.gbft.framework.plugins.InitializablePluginInterface;
import com.gbft.framework.plugins.PluginManager;
import com.gbft.framework.statemachine.StateMachine;
import com.gbft.framework.utils.BenchmarkManager;
import com.gbft.framework.utils.Config;
import com.gbft.framework.utils.DataUtils;
import com.gbft.framework.utils.EntityMapUtils;
import com.gbft.framework.utils.Printer;
import com.gbft.framework.utils.RandomDataStream;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

public class CoordinatorUnit extends CoordinatorBase {

    public static int unit_id;

    private int myUnit;

    private Map<Integer, Entity> entities;
    private Map<Integer, Connection> connections;
    private AtomicInteger connected_units;
    private LinkedBlockingQueue<Event> inQueueClient = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<Event> inQueueReplica = new LinkedBlockingQueue<>();
    private Thread receiveFromInQueueClient;
    private Thread receiveFromInQueueReplica;
    public final long MESSAGE_WAIT_TIME = 100;
    protected BenchmarkManager benchmarkManager;

    public String defaultProtocol;
    public int port;

    public CoordinatorUnit(
            int port,
            int unit,
            int nodeCount,
            int clientCount,
            String coordinationServerAddress,
            int clusterNum) {
        super(port);
        this.port = port;
        this.clusterNum = clusterNum;
        myUnit = unit;
        unit_id = unit;

        entities = new HashMap<>();
        connections = new ConcurrentHashMap<>();
        connected_units = new AtomicInteger(0);

        var split = coordinationServerAddress.split(":");
        unitAddressMap.put(SERVER, Pair.of(split[0], Integer.parseInt(split[1])));
        println(unitAddressMap.get(SERVER).getLeft() + " " + unitAddressMap.get(SERVER).getRight());
        var unitData = DataUtils.createUnitData(unit, nodeCount, clientCount, clusterNum);
        var initEvent = DataUtils.createEvent(unitData);
        sendEvent(SERVER, initEvent);
        println("Send event completed");
    }

    public int getMyUnit(){
        return myUnit;
    }

    public int getClusterNum(){
        return clusterNum;
    }

    @Override
    public void receiveEvent(Event event, Socket socket) {
        var coordinationType = event.getEventType();
        // if (coordinationType == EventType.INIT_SHARD) {
        //     println("Received new data at new clienty *** ");
        //     println(event.getInitShardData().toString());
        //     println("y  oyoyoyoyoo yoyoyoo *** ");
        // }
        if (coordinationType == EventType.CONFIG) {
            /*
             * initFromConfig mainly initializes the config map data and the default protocol
             * and adds units to entity map util.
             */
            initFromConfig(event.getConfigData().getDataMap(), event.getConfigData().getDefaultProtocol(),
                    event.getConfigData().getUnitsList());
            defaultProtocol = event.getConfigData().getDefaultProtocol();
            Config.setCurrentProtocol(defaultProtocol);

            var clientType = Config.string("benchmark.client");
            
            /*
             * Now it gets the runner for the id and creates the required mapping object
             */
            EntityMapUtils.getUnitClients(myUnit).forEach((id) -> entities.put(id, genClient(clientType, id)));
            EntityMapUtils.getUnitNodes(myUnit).forEach((id) -> entities.put(id, new Node(id, this)));
            
            benchmarkManager = new BenchmarkManager(null);

            /*
             * Now iterates over all units in entity map util and sends a connection
             * request and updates connections map using ID as key and connection object as
             * value
             */
            for (var unit : EntityMapUtils.getAllUnits()) {
                if (unit != myUnit) {
                    var connection = new Connection(this, myUnit, unit, inQueueClient, inQueueReplica,
                            benchmarkManager);
                    connections.put(unit, connection);
                }
            }
            var unitData = DataUtils.createUnitData(myUnit, 1, 0, this.clusterNum);
            var allReadyEvent = DataUtils.createEvent(unitData, EventType.READY);
            sendEvent(SERVER, allReadyEvent);

            println("Unit configured.");
        } else if (coordinationType == EventType.PLUGIN_INIT) {
            var data = event.getPluginData();
            //var targets = data.getTargetsCount() == 0 ? entities.keySet() : data.getTargetsList();
            /*
            target here means runner. In case we are sending message to client ( runner = 4), we put the id
            as the source of the message. This is because client will get requests from all.
            When we are sending to just the cluster nodes, we use the runner of the coordinator.
            */ 
            var targets = data.getTargetsCount() == 0 ? entities.keySet() : data.getTargetsList().contains(4)?List.of(16):data.getTargetsList();
            println("Received plugin event for targets: " + targets.toString() + ".");
            println("Entities map for: "+entities);
            println("Target list is: "+targets);
            /*
             * The relevant plugin is mainly the MacMessagePlugin which is used to generate
             * the secret keys for the nodes and clients. A unit updates their map and 
             * sends a request to all folllowing runners to updates their secret key map
             */
            for (var id: targets) {
                var entity = entities.get(id);
                // println(entity.toString() + " " + entity.getId() + " " + entity.isClient() + " " + entity.isPrimary());
                
                var plugins = entity.getMessagePlugins();
                for (var plugin : plugins) {
                    if (plugin instanceof InitializablePluginInterface initPlugin) {
                        initPlugin.handleInitEvent(event.getPluginData());
                    }
                }
            }

            /*
             * Waits for all secret keys from the units in the EntityMap utils to be updated
             * and finish the initialization of the plugin.
             */
            var finished = true;
            for (var entity : entities.values()) {
                println("Ent is: " + entity.getId());
                var plugins = entity.getMessagePlugins();
                for (var plugin : plugins) {
                    if (plugin instanceof InitializablePluginInterface initPlugin) {
                        println("Init plugin is: " + initPlugin.toString());
                        finished = finished && initPlugin.isInitialized();
                    }
                }
            }
            println("Plug initialised status: "+finished);
            if (finished) {
                println("Inside finished block.");
                println("Id is "+myUnit+" and clusterNum is "+this.clusterNum);
                var unitData = DataUtils.createUnitData(myUnit, 1, 0, this.clusterNum);
                var readyEvent = DataUtils.createEvent(unitData, EventType.READY);
                // superSendEvent(SERVER, readyEvent);
                sendEvent(SERVER, readyEvent);
            }
        } else if (coordinationType == EventType.CONNECTION) {
            if (event.getTarget() == SERVER) {
                /*
                 * If the event is from the server, create connections to all other units.
                 */
                println("Received connection event from server.");
                for (var connection : connections.values()) {
                    if (connection.createSocket()) {
                        connected_units.incrementAndGet();
                    }
                }

                /*
                 * Wait for all connections to be established before starting the sender and
                 * receiver threads.
                 */
                while (connected_units.get() < EntityMapUtils.unitCount() - 1){
                    // println("Waiting for connections to complete. Current connections count : "+connected_units.get());
                    // println("Expected connections: "+EntityMapUtils.unitCount());
                };

                connections.values().forEach(connection -> connection.startSenderReceiver());
                receiveFromInQueueClient = new Thread(new ReceiverPoller(inQueueClient));
                receiveFromInQueueClient.start();
                receiveFromInQueueReplica = new Thread(new ReceiverPoller(inQueueReplica));
                receiveFromInQueueReplica.start();

                var unitData = DataUtils.createUnitData(myUnit, 1, 0, this.clusterNum);
                var readyEvent = DataUtils.createEvent(unitData, EventType.READY);
                sendEvent(SERVER, readyEvent);
                println("Connection initialized.");
            } else {
                println("Received connection event from unit " + event.getTarget() + ".");
                connections.get(event.getTarget()).createSocket(socket);
                connected_units.incrementAndGet();
            }
        } else if (coordinationType == EventType.START) {
            for (var entity : entities.values()) {
                entity.start();
            }
            benchmarkManager.start();

            println("Benchmark started.");
        } else if (coordinationType == EventType.BENCHMARK_REPORT) {

            // println("Sending benchmark results to server.");

            var reportData = ReportData.newBuilder();
            for (var id : entities.keySet()) {
                var map = entities.get(id).reportBenchmark();
                var item = ReportItem.newBuilder().putAllItemData(map).build();
                var name = (entities.get(id).isClient() ? "Client " : "Node ") + id;
                reportData.putReportData(name, item);
            }
            // var map = reportBenchmark();
            // var item = ReportItem.newBuilder().putAllItemData(map).build();
            // reportData.putReportData("CoordinatorUnit " + myUnit, item);

            var reportEvent = DataUtils.createEvent(reportData.build());

            sendEvent(SERVER, reportEvent);

            // println(Printer.convertToString(reportEvent.getReportData()));
        } else if (coordinationType == EventType.STOP) {
            for (var entity : entities.values()) {
                entity.stop();
            }
            connections.values().forEach(connection -> connection.closeConnection());

            Printer.flush();

            println("Unit execution stopped.");
            stop();
            receiveFromInQueueClient.interrupt();
            receiveFromInQueueReplica.interrupt();
        } else if (event.getEventType() == EventType.MESSAGE) {
            println("Message event received in "+myUnit);
            var messages = event.getMessageBlock().getMessageDataList();

            // TODO: More parallel.
            for (var message : messages) {
                var targets = message.getTargetsList();
                for (var target : targets) {
                    if (target == message.getSource() || EntityMapUtils.getUnit(target) != myUnit) {
                        continue;
                    }
                    println("Processing message for target: "+target);

                    // TODO: Use Virtual Thread.

                    // in-dark attack
                    if (message.getFault().getBlockedTargetsList().contains(target))
                        continue;
                    // timeout attack
                    var delay = 0L;
                    if (message.getFault().getDelayedTargetsList().contains(target))
                        delay = message.getFault().getDelay();
                    final long _delay = delay;
                    // execute
                    new Thread(() -> {
                        try {
                            if (_delay > 0L)
                                Thread.sleep(_delay);
                            entities.get(target).handleMessage(message);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }).start();
                }
            }
        } 
    }

    protected class ReceiverPoller implements Runnable {

        private LinkedBlockingQueue<Event> inQueue;

        protected ReceiverPoller(LinkedBlockingQueue<Event> inQueue) {
            this.inQueue = inQueue;
        }

        @Override
        public void run() {
            while (isRunning) {
                try {
                    var event = this.inQueue.poll(MESSAGE_WAIT_TIME, TimeUnit.MILLISECONDS);

                    if (event != null) {
                        if (event.getEventType() == EventType.MESSAGE) {
                            System.out.println("Message event received in "+myUnit);
                            var messages = event.getMessageBlock().getMessageDataList();

                            for (var message : messages) {
                                var targets = message.getTargetsList();
                                System.out.println("Targets: "+targets.toString() + " for message: "+message.toString());
                                for (var target : targets) {
                                    var new_target = target%4;
                                    if (target == message.getSource() || EntityMapUtils.getUnit(new_target) != myUnit) {
                                        continue;
                                    }

                                    // if (target == message.getSource() || target != myUnit) {
                                    //     continue;
                                    // }

                                    // in-dark attack
                                    if (message.getFault().getBlockedTargetsList().contains(new_target))
                                        continue;
                                    // timeout attack
                                    var delay = 0L;
                                    if (message.getFault().getDelayedTargetsList().contains(new_target))
                                        delay = message.getFault().getDelay();
                                    final long _delay = delay;
                                    // execute
                                    new Thread(() -> {
                                        try {
                                            if (_delay > 0L)
                                                Thread.sleep(_delay);
                                            entities.get(new_target).handleMessage(message);
                                        } catch (InterruptedException e) {
                                            e.printStackTrace();
                                        }
                                    }).start();
                                }
                            }
                        }

                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void superSendEvent(int unit, Event event) {
        if (unit == myUnit) {
            receiveEvent(event, null);
        } else {
            super.sendEvent(unit, event);
        }
    }

    @Override
    public void sendEvent(int unit, Event event) {
        if (unit == myUnit) {
            receiveEvent(event, null);
        } else {
            if (unit != SERVER) {
                System.out.println("Sending event to " + unit + " at " + unitAddressMap.get(unit).getLeft() + ":"
                        + unitAddressMap.get(unit).getRight());
                /*
                 * Adds the benchmark data for the coordinator unit send event
                 */
                benchmarkManager.add(BenchmarkManager.COORDINATOR_UNIT_SEND, 0, System.nanoTime());
            }
            /*
             * If the unit is in the connections map, send the event using the connection
             */
            if (connections.containsKey(unit)) {
                System.out.println("Using connection - Sending event to " + unit + " at " + unitAddressMap.get(unit).getLeft() + ":"
                        + unitAddressMap.get(unit).getRight());
                connections.get(unit).send(event);
            } else {
                /*
                 * If the unit is not in the connections map, send the event using the super
                 */
                System.out.println("Sending event to " + unit + " at " + unitAddressMap.get(unit).getLeft() + ":"
                        + unitAddressMap.get(unit).getRight());
                super.sendEvent(unit, event);
            }
        }
    }

    @Override
    public void sendEvent(List<Integer> units, Event event) {
        for (var unit : units) {
            sendEvent(unit, event);
        }
    }

    public void sendMessages(List<MessageData> messages, int sender) {
        /*
         * Gets all the id's of units in the target list
         */
        var units = messages.parallelStream().flatMap(message -> message.getTargetsList().stream())
                .map(target -> EntityMapUtils.getUnit(target))
                .distinct()
                .toList();
        println("Found target: "+units.toString());
        List<MessageData> transformedMessages = new ArrayList<>();

        Entity senderEntity = this.entities.get(sender);
        // fault implementation
        if (senderEntity.isPrimary()) {
            for (var message : messages) {
                var faultDataBuilder = FaultData.newBuilder()
                        .addAllDelayedTargets(senderEntity.getTimeoutFault().getAffectedEntities())
                        .setDelay(senderEntity.getTimeoutFault().getDelay());

                if (senderEntity.getInDarkFault().getApply()) {
                    faultDataBuilder = faultDataBuilder
                            .addAllBlockedTargets(senderEntity.getInDarkFault().getAffectedEntities());
                }
                transformedMessages.add(message.toBuilder().setFault(faultDataBuilder).build());
            }
        } else {
            transformedMessages = messages;
        }

        List<MessageData> sizeTransformedMessages = new ArrayList<>();
        for (var message : transformedMessages) {
            if (message.getMessageType() == StateMachine.REPLY) {
                var requests = message.getRequestsList();
                var m = message.toBuilder().clearRequests();
                for (var request : requests) {
                    try {
                        m.addRequests(
                                request.toBuilder().clearRequestDummy().setRequestDummy(
                                        ByteString.readFrom(
                                                new RandomDataStream(request.getReplySize()))));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                sizeTransformedMessages.add(m.build());
            } else {
                sizeTransformedMessages.add(message);
            }
        }

        var event = DataUtils.createEvent(sizeTransformedMessages);
        sendEvent(units, event);
    }

    public void initFromConfig(Map<String, String> configContent, String defaultProtocol, List<UnitData> unitData) {
        initFromConfig(configContent, defaultProtocol);

        Printer.init();
        PluginManager.initDefaultPlugins();
        unitData.forEach(item -> EntityMapUtils.addUnitData(item));
    }

    private ShardingClient genClient(String type, int id) {
        //return type.equals("basic") ? new Client(id, this) : new DynamicClient(id, this);
        println("Creating sharding client instance" + id + ".");
        return new ShardingClient(id, this);
    }

    protected int reportnum = 0;

    public Map<String, String> reportBenchmark() {
        var benchmark = benchmarkManager.getBenchmarkById(reportnum);

        var report = new HashMap<String, String>();
        report.put("connection-send", "count: " + benchmark.count(BenchmarkManager.CONNECTION_SEND));
        report.put("connection-begin-send", "count: " + benchmark.count(BenchmarkManager.CONNECTION_BEGIN_SEND));
        report.put("sender-thread-write", "count: " + benchmark.count(BenchmarkManager.SENDER_THREAD_WRITE));
        report.put("receiver-thread-inqueue-client",
                "count: " + benchmark.count(BenchmarkManager.RECEIVER_THREAD_INQUEUE_CLIENT));
        report.put("receiver-thread-inqueue-replica",
                "count: " + benchmark.count(BenchmarkManager.RECEIVER_THREAD_INQUEUE_REPLICA));
        report.put("coordinator-unit-sendevent", "count: " + benchmark.count(BenchmarkManager.COORDINATOR_UNIT_SEND));

        report.put("active threads", "count: " + Thread.activeCount());

        reportnum += 1;
        return report;
    }

    public static void main(String[] args) {
        Options options = new Options();
        var unitOption = new Option("u", "unit", true, "the coordination unit number");
        var portOption = new Option("p", "port", true, "the coordination unit port");
        var nodesOption = new Option("n", "nodes", true, "the number of nodes");
        var clientsOption = new Option("c", "clients", true, "the number of clients");
        var serverOption = new Option("s", "server", true, "the coordination server address");
        var clusterOption = new Option("k", "cluster", true, "the cluster number of all units");
        clusterOption.setType(Number.class);
        unitOption.setType(Number.class);
        unitOption.setRequired(true);
        portOption.setType(Number.class);
        portOption.setRequired(true);
        nodesOption.setType(Number.class);
        clientsOption.setType(Number.class);
        serverOption.setRequired(true);

        options.addOption(unitOption);
        options.addOption(portOption);
        options.addOption(nodesOption);
        options.addOption(clientsOption);
        options.addOption(serverOption);
        options.addOption(clusterOption);
        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse(options, args);
            Number unit = (Number) cmd.getParsedOptionValue("unit");
            Number port = (Number) cmd.getParsedOptionValue("port");
            var clusterNum = (Number) cmd.getParsedOptionValue("cluster");
            var serverAddress = cmd.getOptionValue("server");

            int nodeCount = 0, clientCount = 0;
            if (cmd.hasOption("nodes")) {
                var nodeOp = (Number) cmd.getParsedOptionValue("nodes");
                if (nodeOp.intValue() > 0) {
                    nodeCount = nodeOp.intValue();
                }
            }
            if (cmd.hasOption("clients")) {
                var clientOp = (Number) cmd.getParsedOptionValue("clients");
                if (clientOp.intValue() > 0) {
                    clientCount = clientOp.intValue();
                }
            }

            new CoordinatorUnit(port.intValue(), unit.intValue(), nodeCount, clientCount, serverAddress,
                    clusterNum.intValue());
        } catch (ParseException e) {
            System.err.println("Command parsing error: " + e.getMessage());
            var formatter = new HelpFormatter();
            formatter.printHelp("Usage:", options);
        }
    }
}
