package org.codingmatters.mr.simulation.exec;

import org.codingmatters.mr.simulation.exec.data.set.StreamDataSet;
import org.codingmatters.mr.simulation.io.FunctionSupplier;

public class MapReduceConfig {
    private final StreamDataSet data;
    private final FunctionSupplier map;
    private final FunctionSupplier reduce;
    private final int mapperCount;

    public MapReduceConfig(StreamDataSet data, FunctionSupplier map, FunctionSupplier reduce, int mapperCount) {
        this.data = data;
        this.map = map;
        this.reduce = reduce;
        this.mapperCount = mapperCount;
    }

    public StreamDataSet getData() {
        return data;
    }

    public FunctionSupplier getMap() {
        return map;
    }

    public FunctionSupplier getReduce() {
        return reduce;
    }

    public int getMapperCount() {
        return mapperCount;
    }
}
