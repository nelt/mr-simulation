package org.codingmatters.mr.simulation.app;

import org.codingmatters.mr.simulation.exec.MapReduceConfig;
import org.codingmatters.mr.simulation.exec.MapReduceExecutor;
import org.codingmatters.mr.simulation.exec.data.set.StreamDataSet;

import java.io.*;
import java.util.*;

public class ConsoleApp {

    public static void main(String[] args) {
        Map<String, String> namedParams = new HashMap<>();
        List<String> rawArgs = new LinkedList<>();

        String name = null;
        for (String arg : args) {
            if(arg.startsWith("--")) {
                name = arg.substring("--".length());
            } else if(name != null) {
                namedParams.put(name, arg);
                name = null;
            } else {
                rawArgs.add(arg);
            }
        }

        if(! namedParams.containsKey("map")) {
            throw new RuntimeException("missing map script (--map)");
        }
        if(! namedParams.containsKey("reduce")) {
            throw new RuntimeException("missing map script (--reduce)");
        }
        if(! namedParams.containsKey("data-set")) {
            throw new RuntimeException("missing map script (--data-set)");
        }


        int mapperCount = 4;
        if(namedParams.containsKey("mapper-count")) {
            try {
                mapperCount = Integer.parseInt(namedParams.get("mapper-count"));
            } catch (NumberFormatException e) {
                throw new RuntimeException("--mapper-count must specify an integer value, was " + namedParams.get("mapper-count"), e);
            }
        }


        Map<String, Map<String, Object>> result;
        long elapsed;
        try(
                InputStream dataSetInput = new FileInputStream(namedParams.get("data-set"));
                StreamDataSet dataSet = new StreamDataSet(dataSetInput)
        ) {
            MapReduceConfig config = new MapReduceConfig(
                    dataSet,
                    () -> new FileReader(namedParams.get("map")),
                    () -> new FileReader(namedParams.get("reduce")),
                    mapperCount);

            try(MapReduceExecutor mapReduceExecutor = new MapReduceExecutor(config)) {
                long start = System.currentTimeMillis();
                result = mapReduceExecutor.execute().get();
                elapsed = System.currentTimeMillis() - start;
            }
        } catch (Exception e) {
            throw new RuntimeException("failed executing M/R algorithm", e);
        }

        List<String> sortedKeys = new LinkedList<>(result.keySet());
        Collections.sort(sortedKeys);

        for (String key : sortedKeys) {
            System.out.printf("%s :: %s\n", key, objectToString(result.get(key)));
        }

        System.out.printf("\n#############################################################\n");
        System.out.printf("Map/Reduce ran in %d ms using %d mappers\n", elapsed, mapperCount);
        System.out.printf("#############################################################\n");
    }

    private static Object objectToString(Map<String, Object> map) {
        return map.toString();
    }
}
