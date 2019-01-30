package org.codingmatters.mr.simulation.app;

import org.codingmatters.mr.simulation.exec.DataSet;
import org.codingmatters.mr.simulation.exec.MapReduceConfig;
import org.codingmatters.mr.simulation.exec.MapReduceExecutor;

import java.io.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
                DataSet dataSet = new DataSet(dataSetInput);
                Reader mapReader = new FileReader(namedParams.get("map"));
                Reader reduceReader = new FileReader(namedParams.get("reduce"));
        ) {
            MapReduceConfig config = new MapReduceConfig(
                    dataSet,
                    mapReader,
                    reduceReader
            );

            try(MapReduceExecutor mapReduceExecutor = new MapReduceExecutor(config)) {
                result = mapReduceExecutor.execute().get();
            }
        } catch (Exception e) {
            throw new RuntimeException("failed executing M/R algorithm", e);
        }

        for (String key : result.keySet()) {
            System.out.printf("%s :: %s\n", key, result.get(key));
        }

    }
}
