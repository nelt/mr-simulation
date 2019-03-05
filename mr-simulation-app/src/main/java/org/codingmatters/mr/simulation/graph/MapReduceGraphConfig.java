package org.codingmatters.mr.simulation.graph;

import java.util.Arrays;
import java.util.LinkedList;

public class MapReduceGraphConfig {
    private final int [] reducerCountByPhase;
    private final int [] mapperCountPerPhaseOneReducer;

    public MapReduceGraphConfig(int mapperCount, int reducePhaseCount) {
        int mr = mapperCount / reducePhaseCount;
        int mmr = mapperCount / mr;

        int phaseRatio = mr <= mmr ? mr : mmr;

        this.reducerCountByPhase = new int[reducePhaseCount];
        this.reducerCountByPhase[this.reducerCountByPhase.length - 1] = 1;

        for (int i = this.reducerCountByPhase.length - 2; i >= 0; i--) {
            this.reducerCountByPhase[i] = this.reducerCountByPhase[i + 1] * phaseRatio;
        }

        this.mapperCountPerPhaseOneReducer = new int[this.reducerCountByPhase[0]];
        int q = mapperCount / this.reducerCountByPhase[0];
        int r = mapperCount % this.reducerCountByPhase[0];

        for (int i = 0; i < this.mapperCountPerPhaseOneReducer.length; i++) {
            if(i >= r) {
                this.mapperCountPerPhaseOneReducer[i] = q;
            } else {
                this.mapperCountPerPhaseOneReducer[i] = q + 1;
            }
        }
    }

    public int[] getReducerCountByPhase() {
        return this.reducerCountByPhase;
    }

    public int[] getMapperCountPerPhaseOneReducer() {
        return mapperCountPerPhaseOneReducer;
    }

    @Override
    public String toString() {
        return "MapReduceGraphConfig{" +
                "reducerCountByPhase=" + Arrays.toString(reducerCountByPhase) +
                ", mapperCountPerPhaseOneReducer=" + Arrays.toString(mapperCountPerPhaseOneReducer) +
                '}';
    }

    public Node lastNode() {
        Node.ReducerNode.Builder result = Node.last();
        this.appendPreviousNodes(result, this.reversedReducers(), this.mappers());


        return result.build();
    }

    private void appendPreviousNodes(Node.ReducerNode.Builder current, LinkedList<Integer> reversedReducers, LinkedList<Integer> mappers) {
        if(! reversedReducers.isEmpty()) {
            Integer reducerCount = reversedReducers.pop();
            for (int i = 0; i < reducerCount; i++) {
                Node.ReducerNode.Builder previous = Node.reducer();
                this.appendPreviousNodes(previous, new LinkedList<>(reversedReducers), mappers);
                current.withPrevious(previous.build());
            }
        } else {
            Integer mapperCount = mappers.pop();
            for (int i = 0; i < mapperCount; i++) {
                current.withPrevious(Node.mapper());
            }
        }
    }

    private LinkedList<Integer> reversedReducers() {
        LinkedList<Integer> result = new LinkedList<>();
        for (int i = this.reducerCountByPhase.length - 2; i >= 0; i--) {
            result.add(this.reducerCountByPhase[i] / this.reducerCountByPhase[i+1]);
        }
        return result;
    }

    private LinkedList<Integer> mappers() {
        LinkedList<Integer> result = new LinkedList<>();
        for (int i : this.mapperCountPerPhaseOneReducer) {
            result.add(i);
        }
        return result;
    }
}
