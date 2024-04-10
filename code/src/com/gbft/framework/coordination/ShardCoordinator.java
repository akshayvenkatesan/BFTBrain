package com.gbft.framework.coordination;

import java.net.Socket;
import java.io.File;
import java.io.IOException;
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

import com.gbft.framework.data.Event;
import com.gbft.framework.data.Event.EventType;
import com.gbft.framework.utils.Config;
import com.gbft.framework.utils.ConfigObject;
import com.gbft.framework.utils.DataUtils;
import com.gbft.framework.utils.EntityMapUtils;
import com.gbft.framework.utils.Printer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;

public class ShardCoordinator extends CoordinatorBase {

    public ShardCoordinator(int port) {
        super(port);
        clusterNum = 0;
    }

    @Override
    protected void receiveEvent(Event event, Socket socket) {
        var eventType = event.getEventType();
        if (eventType == EventType.INIT) {
            var unitData = event.getUnitData();
            println("**********************");
            println("Cluster num of unit " + unitData.getUnit() + " is " + unitData.getClusterNum());   
            println("Registered unit " + unitData.getUnit() + "at the shard coordinator.");    
            println("**********************");  
        }

    }

    public void run() {
        try (var scanner = new Scanner(System.in)) {
            while (isRunning) {
                System.out.print("$ server running");

                var command = System.console().readLine();
            }
        }

        stop();
    }
    
    public static void main(String[] args) {
        Options options = new Options();

        var portOption = new Option("p", "port", true, "the Shard coordinator port");
        portOption.setType(Number.class);
        portOption.setRequired(true);
        options.addOption(portOption);

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse(options, args);
            Number port = (Number) cmd.getParsedOptionValue("port");
            new ShardCoordinator(port.intValue()).run();
        } catch (ParseException e) {
            System.err.println("Command parsing error: " + e.getMessage());
            var formatter = new HelpFormatter();
            formatter.printHelp("Usage:", options);
        }
    }
    
}
