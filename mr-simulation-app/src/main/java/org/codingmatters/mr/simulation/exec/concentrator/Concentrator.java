package org.codingmatters.mr.simulation.exec.concentrator;

import org.codingmatters.mr.simulation.exec.exceptions.MapReduceException;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class Concentrator {

    private final MapReduceConsumer whenConcentrated;
    private final int sourceCount;
    private int takeCount = 0;

    private final Map<String, List<Map<String, Object>>> concentrated = new HashMap<>();

    public Concentrator(MapReduceConsumer whenConcentrated, int sourceCount) {
        if(sourceCount <= 0) {
            throw new IllegalArgumentException("concentrator must have at least one source");
        }
        this.whenConcentrated = whenConcentrated;
        this.sourceCount = sourceCount;
    }

    public synchronized Concentrator take(Map<String, List<Map<String, Object>>> values) throws MapReduceException {
        if(this.takeCount >= this.sourceCount) {
            throw new IllegalStateException("concentrator is already consumed, should not be called more then source count (" + this.sourceCount +")");
        }
        this.takeCount ++;

        this.merge(values);

        if(this.takeCount == this.sourceCount) {
            this.whenConcentrated.accept(this.concentrated);
        }
        return this;
    }

    private void merge(Map<String, List<Map<String, Object>>> values) {
        for (String key : values.keySet()) {
            if(! this.concentrated.containsKey(key)) {
                this.concentrated.put(key, new LinkedList<>());
            }
            this.concentrated.get(key).addAll(values.get(key));
        }
    }

    @FunctionalInterface
    public interface MapReduceConsumer {
        void accept(Map<String, List<Map<String, Object>>> values) throws MapReduceException;
    }
}
