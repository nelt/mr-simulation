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

        Map<String, Map<String, Object>> result;

        try(
                InputStream dataSetInput = new FileInputStream(namedParams.get("data-set"));
                StreamDataSet dataSet = new StreamDataSet(dataSetInput)
        ) {
            MapReduceConfig config = new MapReduceConfig(
                    dataSet,
                    () -> new FileReader(namedParams.get("map")),
                    () -> new FileReader(namedParams.get("reduce"))
            );

            try(MapReduceExecutor mapReduceExecutor = new MapReduceExecutor(config)) {
                result = mapReduceExecutor.execute().get();
            }
        } catch (Exception e) {
            throw new RuntimeException("failed executing M/R algorithm", e);
        }

        List<String> sortedKeys = new LinkedList<>(result.keySet());
        Collections.sort(sortedKeys);

        for (String key : sortedKeys) {
            System.out.printf("%s :: %s\n", key, result.get(key));
        }

    }
}
