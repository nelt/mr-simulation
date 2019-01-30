package org.codingmatters.mr.simulation.exec;

import org.codingmatters.mr.simulation.exec.exceptions.MapReduceException;

import java.io.Reader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;

public class MapReduceExecutor implements AutoCloseable {

    private DataSet data;
    private final ExecutorService pool = Executors.newFixedThreadPool(2);
    private final Reader mapFunctionReader;
    private final Reader reduceFunctionReader;

    public MapReduceExecutor(MapReduceConfig mapReduceConfig) {
        this.data = mapReduceConfig.getData();
        mapFunctionReader = mapReduceConfig.getMap();
        reduceFunctionReader = mapReduceConfig.getReduce();
    }

    public Future<Map<String, Map<String, Object>>> execute() {
        Callable<Map<String, Map<String, Object>>> executor = () -> doExecute();
        return this.pool.submit(executor);
    }

    private Map<String, Map<String, Object>> doExecute() throws MapReduceException {
        Map<String, List<Map<String, Object>>> emittedValues;
        try {
            emittedValues = this.pool.submit(this.mapper()).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new MapReduceException("error in mapping phase", e);
        }

        Map<String, Map<String, Object>> result = new HashMap<>();
        for (String key : emittedValues.keySet()) {
            Map<String, Object> reduced = null;
            try {
                reduced = this.pool.submit(this.reducer(emittedValues.get(key))).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new MapReduceException("error in reduce phase", e);
            }
            result.put(key, reduced);
        }

        return result;
    }

    private Callable<Map<String, List<Map<String, Object>>>> mapper() {
        return () -> {
            Mapper mapper = new Mapper(this.mapFunctionReader);

            for(Optional<Map<String, Object>> datum = data.next(); datum.isPresent(); datum = data.next()) {
                mapper.map(datum.get());
            }

            Map<String, List<Map<String, Object>>> result = new HashMap<>();
            for (String emittedKey : mapper.emittedKeys()) {
                result.put(emittedKey, mapper.emittedFor(emittedKey));
            }

            return result;
        };
    }

    private Callable<Map<String, Object>> reducer(List<Map<String, Object>> values) {
        return () -> {
            Reducer reducer = new Reducer(this.reduceFunctionReader);
            return reducer.reduce(values);
        };
    }


    @Override
    public void close() throws Exception {
        this.data.close();
        this.closePool();
    }

    private void closePool() {
        this.pool.shutdown();
        try {
            this.pool.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {}
        if(! this.pool.isTerminated()) {
            this.pool.shutdownNow();
        }
    }
}
