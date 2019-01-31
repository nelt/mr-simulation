package org.codingmatters.mr.simulation.exec;

import org.codingmatters.mr.simulation.io.FunctionSupplier;

public class MapReduceConfig {
    private final DataSet data;
    private final FunctionSupplier map;
    private final FunctionSupplier reduce;

    public MapReduceConfig(DataSet data, FunctionSupplier map, FunctionSupplier reduce) {
        this.data = data;
        this.map = map;
        this.reduce = reduce;
    }

    public DataSet getData() {
        return data;
    }

    public FunctionSupplier getMap() {
        return map;
    }

    public FunctionSupplier getReduce() {
        return reduce;
    }
}
