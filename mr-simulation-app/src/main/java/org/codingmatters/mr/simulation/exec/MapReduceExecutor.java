package org.codingmatters.mr.simulation.exec;

import org.codingmatters.mr.simulation.exec.concentrator.Concentrator;
import org.codingmatters.mr.simulation.exec.data.set.StreamDataSet;
import org.codingmatters.mr.simulation.exec.exceptions.MapReduceException;
import org.codingmatters.mr.simulation.exec.exceptions.MapperException;
import org.codingmatters.mr.simulation.graph.MapReduceGraphConfig;
import org.codingmatters.mr.simulation.graph.Node;
import org.codingmatters.mr.simulation.graph.NodeVisitor;
import org.codingmatters.mr.simulation.io.FunctionSupplier;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

public class MapReduceExecutor implements AutoCloseable {

    private final MapReduceConfig config;
    private StreamDataSet data;
    private final ExecutorService pool;
    private final FunctionSupplier mapFunctionReader;
    private final FunctionSupplier reduceFunctionReader;

    public MapReduceExecutor(MapReduceConfig mapReduceConfig) {
        this.config = mapReduceConfig;
        this.data = mapReduceConfig.getData();
        mapFunctionReader = mapReduceConfig.getMap();
        reduceFunctionReader = mapReduceConfig.getReduce();

        this.pool = Executors.newFixedThreadPool(1 + mapReduceConfig.getMapperCount());
    }

    public Future<Map<String, Map<String, Object>>> execute() {
        Callable<Map<String, Map<String, Object>>> executor = () -> doExecute();
        return this.pool.submit(executor);
    }

    private Map<String, Map<String, Object>> doExecute() throws MapReduceException {
        AtomicReference<Map<String, Map<String, Object>>> result = new AtomicReference<>(new HashMap<>());

        this.starupAndWait(this.prepareExecutionGraph(result));

        return result.get();
    }

    private List<Callable<Map<String, List<Map<String, Object>>>>> prepareExecutionGraph(AtomicReference<Map<String, Map<String, Object>>> result) throws MapReduceException {
        MapReduceGraphConfig graphConfig = new MapReduceGraphConfig(this.config.getMapperCount(), this.config.getReducePhaseCount());
        List<Callable<Map<String, List<Map<String, Object>>>>> mappers = new ArrayList<>(this.config.getMapperCount());
        Stack<Concentrator> concentratorStack = new Stack<>();
        List<MapperException> exceptions = new LinkedList<>();

        graphConfig.lastNode().accept(new NodeVisitor() {
            @Override
            public void visit(Node.MapperNode node) {
                Concentrator concentrator = concentratorStack.peek();
                try {
                    mappers.add(mapper(concentrator));
                } catch (MapperException e) {
                    exceptions.add(e);
                }
            }

            @Override
            public void visit(Node.ReducerNode node) {
                Concentrator nextConcentrator = concentratorStack.pop();

                concentratorStack.push(new Concentrator(
                        reducerConsumer(nextConcentrator),
                        node.previous().length
                ));

                Arrays.stream(node.previous()).forEach(n -> n.accept(this));

                concentratorStack.push(nextConcentrator);
            }

            @Override
            public void visit(Node.LastReducerNode node) {
                concentratorStack.push(
                        new Concentrator(
                                lastReducerConsumer(result),
                                node.previous().length
                        )
                );

                Arrays.stream(node.previous()).forEach(n -> n.accept(this));
            }
        });

        if(! exceptions.isEmpty()) {
            throw new MapReduceException("", exceptions);
        }
        return mappers;
    }

    private void starupAndWait(List<Callable<Map<String, List<Map<String, Object>>>>> mappers) throws MapReduceException {
        List<Future<Map<String, List<Map<String, Object>>>>> mapperResults;
        try {
            mapperResults = this.pool.invokeAll(mappers);
        } catch (InterruptedException e) {
            throw new MapReduceException("error invoking map phase", e);
        }

        for (Future<Map<String, List<Map<String, Object>>>> mapFuture : mapperResults) {
            try {
                mapFuture.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new MapReduceException("error mapping values", e);
            }
        }
    }

    private Concentrator.MapReduceConsumer lastReducerConsumer(AtomicReference<Map<String, Map<String, Object>>> result) {
        return concentrated -> {
                for (String key : concentrated.keySet()) {
                    Map<String, Object> reduced = null;
                    try {
                        Callable<Map<String, Object>> reducer = this.reducer(concentrated.get(key));
                        reduced = reducer.call();
                    } catch (Exception e) {
                        throw new MapReduceException("error in reduce phase", e);
                    }
                    result.get().put(key, reduced);
                }
            };
    }

    private Concentrator.MapReduceConsumer reducerConsumer(Concentrator concentrator) {
        return concentrated -> {
            Map<String, List<Map<String, Object>>> result = new HashMap<>();
            for (String key : concentrated.keySet()) {
                Map<String, Object> reduced = null;
                try {
                    Callable<Map<String, Object>> reducer = this.reducer(concentrated.get(key));
                    reduced = reducer.call();
                } catch (Exception e) {
                    throw new MapReduceException("error in reduce phase", e);
                }
                result.put(key, Arrays.asList(reduced));
            }
            concentrator.take(result);
        };
    }

    private Callable<Map<String, List<Map<String, Object>>>> mapper(Concentrator concentrator) throws MapperException {
        Mapper mapper = new Mapper(this.mapFunctionReader);
        return () -> {

            for(Optional<Map<String, Object>> datum = data.next(); datum.isPresent(); datum = data.next()) {
                mapper.map(datum.get());
            }

            Map<String, List<Map<String, Object>>> result = new HashMap<>();
            for (String emittedKey : mapper.emittedKeys()) {
                result.put(emittedKey, mapper.emittedFor(emittedKey));
            }

            concentrator.take(result);
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
