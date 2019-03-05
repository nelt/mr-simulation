package org.codingmatters.mr.simulation.exec;

import org.codingmatters.mr.simulation.exec.data.set.StreamDataSet;
import org.codingmatters.mr.simulation.io.FunctionSupplier;

public class MapReduceConfig {
    private final StreamDataSet data;
    private final FunctionSupplier map;
    private final FunctionSupplier reduce;
    private final int mapperCount;
    private int reducePhaseCount;

    public MapReduceConfig(StreamDataSet data, FunctionSupplier map, FunctionSupplier reduce, int mapperCount, int reducePhaseCount) {
        this.data = data;
        this.map = map;
        this.reduce = reduce;
        this.mapperCount = mapperCount;
        this.reducePhaseCount = reducePhaseCount;

        if(this.reducePhaseCount > this.mapperCount) {
            throw new IllegalArgumentException("mapper count must be greater than reduce phases");
        }
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

    public int getReducePhaseCount() {
        return reducePhaseCount;
    }
}
