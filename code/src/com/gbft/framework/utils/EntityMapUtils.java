package com.gbft.framework.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gbft.framework.data.UnitData;

public class EntityMapUtils {

    private static List<UnitData> unitDataList;
    private static Map<Integer, List<UnitData>> unitDataClusterList;
    private static Map<Integer, List<Integer>> clusterServerMapping;
    /*
     * Maintains list of id's
     */
    private static List<Integer> units;
    /*
     * Maintains list of runners for nodes
     */
    private static List<Integer> nodes;
    /*
     * Maintains list of runners for client
     */
    private static List<Integer> clients;
    /*
     * Maintains id to runner node mapping
     */
    private static Map<Integer, List<Integer>> unitNodes;
    /*
     * Maintains id to runner client mapping
     */
    private static Map<Integer, List<Integer>> unitClients;
    /*
     * Maintains runner to id mapping
     */
    private static Map<Integer, Integer> entityUnitMap;

    public static void addUnitData(UnitData data) {
        var unit = data.getUnit();
        println("Adding unit " + unit + " to entity map.");
        units.add(unit);
        units.sort((a, b) -> a - b);
        unitNodes.put(unit, new ArrayList<>());
        unitClients.put(unit, new ArrayList<>());
        unitDataList.add(data);
        unitDataList.sort((a, b) -> a.getUnit() - b.getUnit());
        unitDataClusterList.putIfAbsent(data.getClusterNum(), new ArrayList<>());
        unitDataClusterList.get(data.getClusterNum()).add(data);
        unitDataClusterList.get(data.getClusterNum()).sort((a, b) -> a.getUnit() - b.getUnit());
        println("Getting cluster number for unit " + unit + ".");
        var cluster = data.getClusterNum();
        println("Adding unit " + unit + " to cluster " + cluster + ".");
        clusterServerMapping.putIfAbsent(cluster, new ArrayList<>());
        println("Cluster server mapping: " + clusterServerMapping.toString() + ".");
        clusterServerMapping.get(cluster).add(unit);
        println("Cluster server mapping: " + clusterServerMapping.toString() + ".");

        var runner = nodes.size() + clients.size();
        for (var i = 0; i < data.getClientCount(); i++) {
            clients.add(runner);
            unitClients.get(unit).add(runner);
            entityUnitMap.put(runner, unit);
            runner += 1;
        }

        for (var i = 0; i < data.getNodeCount(); i++) {
            nodes.add(runner);
            unitNodes.get(unit).add(runner);
            entityUnitMap.put(runner, unit);
            runner += 1;
        }
    }

    protected static void println(String str) {
        // var date = dateFormat.format(new Date(System.currentTimeMillis()));
        System.out.println(str);
    }

    public static int unitCount() {
        return units.size();
    }

    public static int nodeCount() {
        return nodes.size();
    }

    public static int clientCount() {
        return clients.size();
    }

    public static List<Integer> getAllUnits() {
        return units;
    }

    public static List<Integer> getClusterUnits(int cluster) {
        return clusterServerMapping.get(cluster);
    }

    public static List<Integer> getAllNodes() {
        return nodes;
    }

    public static List<Integer> getAllClients() {
        return clients;
    }

    public static List<Integer> getUnitNodes(int unit) {
        return unitNodes.get(unit);
    }

    public static List<Integer> getUnitClients(int unit) {
        return unitClients.get(unit);
    }

    public static int getNodeIndex(int nodeId) {
        return nodes.indexOf(nodeId);
    }

    public static int getNodeId(int index) {
        return nodes.get(index);
    }

    public static int getUnit(int entity) {
        return entityUnitMap.get(entity);
    }

    public static List<UnitData> allUnitData() {
        return unitDataList;
    }

    public static List<UnitData> getClusterUnitData(int cluster) {
        return unitDataClusterList.get(cluster);
    }

    public static List<Integer> getclusterServerMapping(int clusternum) {
        return clusterServerMapping.get(clusternum);
    }

    public static Map<Integer, List<Integer>> getAllClusterData() {
        return clusterServerMapping;
    }

    static {
        units = new ArrayList<>();
        nodes = new ArrayList<>();
        clients = new ArrayList<>();
        unitNodes = new HashMap<>();
        unitClients = new HashMap<>();
        entityUnitMap = new HashMap<>();
        unitDataList = new ArrayList<>();
        clusterServerMapping = new HashMap<>();
        unitDataClusterList = new HashMap<>();
    }
}
