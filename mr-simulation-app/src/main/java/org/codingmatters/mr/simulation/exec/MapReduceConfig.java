package org.codingmatters.mr.simulation.exec;

import java.io.Reader;

public class MapReduceConfig {
    private final DataSet data;
    private final Reader map;
    private final Reader reduce;

    public MapReduceConfig(DataSet data, Reader map, Reader reduce) {
        this.data = data;
        this.map = map;
        this.reduce = reduce;
    }

    public DataSet getData() {
        return data;
    }

    public Reader getMap() {
        return map;
    }

    public Reader getReduce() {
        return reduce;
    }
}
