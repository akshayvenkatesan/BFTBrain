package com.gbft.framework.coordination;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;
import java.util.stream.Stream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;

import com.gbft.framework.data.ConfigData;
import com.gbft.framework.data.Event;
import com.gbft.framework.data.InitShardData;
import com.gbft.framework.data.Event.EventType;
import com.gbft.framework.data.InitShardData.ClusterUnits;
import com.gbft.framework.utils.Config;
import com.gbft.framework.utils.ConfigObject;
import com.gbft.framework.utils.DataUtils;
import com.gbft.framework.utils.EntityMapUtils;
import com.gbft.framework.utils.Printer;

public class CoordinatorServer extends CoordinatorBase {
    private String protocol;
    private Map<String, String> configContent;

    private Map<EventType, Integer> responseCounter;
    private Map<Integer, Map<EventType, Integer>> clusterResponseCounter;

    private Benchmarker benchmarker;

    public CoordinatorServer(String protocol, int port, int clusterNum) {
        super(port);
        this.protocol = protocol;
        clusterResponseCounter = new ConcurrentHashMap<>();
        responseCounter = new HashMap<>();
        configContent = new HashMap<>();
        this.clusterNum = clusterNum;

        try {
            var frameworkConfig = Files
                    .readString(Path.of(String.format("../config/config.framework.yaml", clusterNum)));
            var protocolPool = new ConfigObject(frameworkConfig, "").stringList("switching.protocol-pool");

            configContent.put("framework", frameworkConfig);

            for (var pname : protocolPool) {
                var protocolConfig = Files.readString(Path.of("../config/config." + pname + ".yaml"));
                configContent.put(pname, protocolConfig);
            }

            initFromConfig(configContent, protocol);
        } catch (IOException e) {
            System.err.println("Error reading config files.");
            System.exit(1);
        }

        benchmarker = new Benchmarker();
    }

    private void initializeAndStartUnits(int clusternum) {

        var units = EntityMapUtils.getclusterServerMapping(clusternum);
        ConfigData configData;
        if (clusternum == 0) {
            configData = DataUtils.createConfigData(configContent, protocol,
                    EntityMapUtils.allUnitData());
        } else {
            var entity_units = EntityMapUtils.getClusterUnitData(clusternum);
            entity_units.addAll(EntityMapUtils.getClusterUnitData(0));
            configData = DataUtils.createConfigData(configContent, protocol,
                    entity_units);
        }
        int responseCount = clusternum == 0 ? 1 : 4;
        // var configData = DataUtils.createConfigData(configContent, protocol,
        // EntityMapUtils.getClusterUnitData(clusternum));
        var configEvent = DataUtils.createEvent(configData);
        println("Sending Event 1 in cluster " + clusternum);
        sendEvent(units, configEvent);
        var unitCount = EntityMapUtils.unitCount();
        waitResponseCluster(EventType.READY, responseCount, clusternum);
        println("Recieved Event 1 in cluster " + clusternum);
        var initPluginsEvent = DataUtils.createEvent(EventType.PLUGIN_INIT);
        println("Sending Event 2 in cluster " + clusternum);
        sendEvent(units, initPluginsEvent);
        waitResponseCluster(EventType.READY, responseCount, clusternum);
        println("Recieved Event 2 in cluster " + clusternum);
        var initConnectionsEvent = DataUtils.createEvent(EventType.CONNECTION, SERVER);
        println("Sending Event 3 in cluster " + clusternum);
        sendEvent(units, initConnectionsEvent);
        waitResponseCluster(EventType.READY, responseCount, clusternum);
        println("Recieved Event 3 in cluster " + clusternum);
        println("Sending Event 4 in cluster " + clusternum);
        var startEvent = DataUtils.createEvent(EventType.START);
        sendEvent(units, startEvent);
        println("\rUnits initialized.       ");
    }

    public void run() {
        System.out.println("Press Enter after all units are connected, to start the benchmark.");
        System.console().readLine();

        System.out.println("Clients: " + EntityMapUtils.getAllClients());
        System.out.println("Nodes: " + EntityMapUtils.getAllNodes());
        System.out.print("Initializing units ...");

        var units = EntityMapUtils.getAllUnits();

        var configData = DataUtils.createConfigData(configContent, protocol, EntityMapUtils.allUnitData());
        var configEvent = DataUtils.createEvent(configData);

        ExecutorService executor = Executors.newFixedThreadPool(5);
        println("Cluster server mapping in CS is: " + EntityMapUtils.getAllClusterData());
        // Submit the task to be executed asynchronously 4 times
        for (int i = 0; i <= 4; i++) {
            final int iCopy = i;
            executor.submit(() -> initializeAndStartUnits(iCopy));
        }

        // Shutdown the executor and wait for tasks to complete
        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.MINUTES)) {
                println("Executor did not terminate in the specified time.");
                executor.shutdownNow();
            } else {
                println("Executor terminated successfully.");
            }
        } catch (InterruptedException e) {
            println(e.getMessage());
            executor.shutdownNow();
        }

        println("All events sent. Sending to client");

        // var clusterData = EntityMapUtils.getAllClusterData();
        // var initShardData = InitShardData.newBuilder();
        // for (var entry: EntityMapUtils.getAllClusterData().entrySet()) {
        // var clus = entry.getKey();
        // var allUnits = entry.getValue();
        // var clusterUnits = ClusterUnits.newBuilder().addAllValues(allUnits).build();
        // initShardData.putClusterData(clus, clusterUnits);
        // // println(initShardData.toString());
        // }

        // // TODO
        // var initShardEvent = DataUtils.createEvent(16, initShardData.build());
        // println("Sending client shard event");
        // sendEvent(16, initShardEvent);
        benchmarker.start();
        println("Benchmark started.");

        println("Available commands: \"stop\"");
        try (var scanner = new Scanner(System.in)) {
            while (isRunning) {
                System.out.print("$ ");

                var command = System.console().readLine();
                if (command.equals("stop")) {
                    isRunning = false;
                } else if (command.startsWith("block")) {
                    var split = command.split(" ");
                    int target = Integer.parseInt(split[1]);
                    var blockEvent = DataUtils.createEvent(EventType.BLOCK, target);
                    sendEvent(units, blockEvent);
                }
            }
        }

        benchmarker.stop();

        println("Stopping all entities.");
        var stopEvent = DataUtils.createEvent(EventType.STOP);
        sendEvent(units, stopEvent);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
        }

        stop();
    }

    @Override
    public void receiveEvent(Event event, Socket socket) {

        var eventType = event.getEventType();
        var unitData = event.getUnitData();
        if (eventType == EventType.INIT) {
            println("Received INIT event from unit " + event.getUnitData().getUnit());
            println("Created unit data for unit " + unitData.getUnit() + " in cluster " + unitData.getClusterNum());
            EntityMapUtils.addUnitData(unitData);
            println("Unit " + unitData.getUnit() + " is connected to the coordinator server.");
            var myunit = unitAddressMap.get(unitData.getUnit());
            var unitData2 = DataUtils.createUnitData(unitData.getUnit(), unitData.getNodeCount(),
                    unitData.getClientCount(), unitData.getClusterNum() * 10000 + myunit.getRight());
            var event2 = DataUtils.createEvent(unitData2);

            // sendEventToShardCoordinator(event2);
        } else if (eventType == EventType.BENCHMARK_REPORT) {
            var report = Printer.convertToString(event.getReportData());
            benchmarker.print(report);
        }

        var requiredEventMap = clusterResponseCounter.getOrDefault(unitData.getClusterNum(), new HashMap<>());
        synchronized (requiredEventMap) {
            println("Received " + eventType + " event from unit " + unitData.getUnit() + " in cluster "
                    + unitData.getClusterNum());
            requiredEventMap.put(eventType, requiredEventMap.getOrDefault(eventType, 0) + 1);
            clusterResponseCounter.put(unitData.getClusterNum(), requiredEventMap);
            requiredEventMap.notify();
        }

        // synchronized (responseCounter) {
        // responseCounter.put(eventType, responseCounter.getOrDefault(eventType, 0) +
        // 1);
        // responseCounter.notify();
        // }

    }

    private void waitResponse(EventType eventType, int expectedCount) {
        synchronized (responseCounter) {
            while (responseCounter.getOrDefault(eventType, 0) < expectedCount) {
                try {
                    responseCounter.wait();
                } catch (InterruptedException e) {
                }
            }

            responseCounter.put(eventType, 0);
        }
    }

    private void waitResponseCluster(EventType eventType, int expectedCount, int clusternum) {
        println("Waiting for " + expectedCount + " responses of type " + eventType + " from cluster " + clusternum);

        var clusterMap = clusterResponseCounter.getOrDefault(clusternum, new HashMap<>());
        synchronized (clusterMap) {

            // var clusterMap = clusterResponseCounter.getOrDefault(clusternum, new
            // HashMap<>());

            while (clusterMap.getOrDefault(eventType, 0) < expectedCount) {
                println(clusterMap.toString() + " " + expectedCount + " " + eventType + " " + clusternum);
                try {
                    // println("going to wait");
                    clusterMap.wait();
                    // println("Done waiting");
                } catch (InterruptedException e) {
                    // println("killing session " + e.getMessage());
                }
            }

            clusterMap.put(eventType, 0);
            clusterResponseCounter.put(clusternum, clusterMap);
        }
        println("Exiting for " + expectedCount + " responses of type " + eventType + " from cluster " + clusternum);
    }

    private class Benchmarker {
        private final String outputFile;
        private Timer benchmarkTimer;
        private final long benchmarkInterval;

        public Benchmarker() {
            benchmarkInterval = Config.integer("benchmark.benchmark-interval-ms");

            var directory = new File("benchmarks/");
            directory.mkdirs();
            var lastBenchmarkId = Stream.of(directory.listFiles())
                    .map(file -> file.getName().split("-")[0])
                    .filter(id -> StringUtils.isNumeric(id))
                    .sorted(Comparator.reverseOrder()).findFirst().orElse("0000");

            var num = Integer.parseInt(lastBenchmarkId) + 1;
            var nextBenchmarkId = StringUtils.leftPad(num + "", 4).replace(' ', '0');
            outputFile = "benchmarks/" + nextBenchmarkId + "-" + protocol + ".txt";

            benchmarkTimer = new Timer();

            printTitle();
        }

        private void start() {
            benchmarkTimer.schedule(new BenchmarkTask(), benchmarkInterval, benchmarkInterval);
        }

        private void stop() {
            benchmarkTimer.cancel();
        }

        private void printTitle() {
            var reqinterval = Config.integer("benchmark.request-interval-micros") * 1000L;
            var blockSize = Config.integer("benchmark.block-size");
            var f = Config.integer("general.f");

            var title = new StringBuilder();
            title.append("== Benchmark Parameters: request-interval (initial)=")
                    .append(Printer.timeFormat(reqinterval, true))
                    .append(", block-size=").append(blockSize).append(", nodes=").append(EntityMapUtils.nodeCount())
                    .append(", f=").append(f).append("\n");

            print(title.toString());
        }

        private void print(String str) {
            try {
                Files.writeString(Path.of(outputFile), str,
                        StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private class BenchmarkTask extends TimerTask {

        private int reportnum = 0;

        @Override
        public void run() {
            if (isRunning) {
                reportnum += 1;
                var t = benchmarker.benchmarkInterval * reportnum;
                benchmarker
                        .print("-- Report #" + reportnum + " @ t=" + Printer.timeFormat(t * 1000000L, true) + " --\n");

                var units = EntityMapUtils.getAllUnits();
                var reportEvent = DataUtils.createEvent(EventType.BENCHMARK_REPORT);
                sendEvent(units, reportEvent);
            }

        }
    }

    public static void main(String[] args) {
        Options options = new Options();

        var portOption = new Option("p", "port", true, "the coordination server port");
        var protocolOption = new Option("r", "protocol", true, "the benchmark protocol");
        var clusterOption = new Option("k", "cluster", true, "the cluster number of all units");
        portOption.setType(Number.class);
        clusterOption.setType(Number.class);
        portOption.setRequired(true);
        protocolOption.setRequired(true);

        options.addOption(portOption);
        options.addOption(protocolOption);
        options.addOption(clusterOption);

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse(options, args);
            Number port = (Number) cmd.getParsedOptionValue("port");
            var protocol = cmd.getOptionValue("protocol");
            var clusterNum = (Number) cmd.getParsedOptionValue("cluster");

            new CoordinatorServer(protocol, port.intValue(), clusterNum.intValue()).run();
        } catch (ParseException e) {
            System.err.println("Command parsing error: " + e.getMessage());
            var formatter = new HelpFormatter();
            formatter.printHelp("Usage:", options);
        }
    }
}
