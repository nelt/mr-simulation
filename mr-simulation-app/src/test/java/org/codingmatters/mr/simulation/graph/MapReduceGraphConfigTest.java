package org.codingmatters.mr.simulation.graph;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class MapReduceGraphConfigTest {

    @Parameterized.Parameters(name = "given {0} mappers, when {1} reduce phases, then expecting reducer config : {2} and mapper config : {3}")
    public static Collection parameters() {
        return Arrays.asList(
                new Object[] {4, 2, Arrays.asList(2, 1), Arrays.asList(2, 2)},
                new Object[] {5, 2, Arrays.asList(2, 1), Arrays.asList(3, 2)},
                new Object[] {11, 2, Arrays.asList(2, 1), Arrays.asList(6, 5)},
                new Object[] {4, 3, Arrays.asList(1, 1, 1), Arrays.asList(4)},
                new Object[] {11, 3, Arrays.asList(9, 3, 1), Arrays.asList(2, 2, 1, 1, 1, 1, 1, 1, 1)},
                new Object[] {16, 3, Arrays.asList(9, 3, 1), Arrays.asList(2, 2, 2, 2, 2, 2, 2, 1, 1)},
                new Object[] {3, 3, Arrays.asList(1, 1, 1), Arrays.asList(3)}
        );
    }

    private final int mapperCount;
    private final int reducePhaseCount;
    private final int [] expectedReducerCountByPhase;
    private final int[] expectedMapperCountPerPhaseOneReducer;

    public MapReduceGraphConfigTest(int mapperCount, int reducePhaseCount, List<Integer> expectedReducerCountByPhase, List<Integer> expectedMapperCountPerPhaseOneReducer) {
        this.mapperCount = mapperCount;
        this.reducePhaseCount = reducePhaseCount;

        this.expectedReducerCountByPhase = new int[expectedReducerCountByPhase.size()];
        for (int i = 0; i < this.expectedReducerCountByPhase.length; i++) {
            this.expectedReducerCountByPhase[i] = expectedReducerCountByPhase.get(i);
        }

        this.expectedMapperCountPerPhaseOneReducer = new int[expectedMapperCountPerPhaseOneReducer.size()];
        for (int i = 0; i < this.expectedMapperCountPerPhaseOneReducer.length; i++) {
            this.expectedMapperCountPerPhaseOneReducer[i] = expectedMapperCountPerPhaseOneReducer.get(i);
        }
    }

    @Test
    public void testConfig() throws Exception {
        MapReduceGraphConfig graphConfig = new MapReduceGraphConfig(this.mapperCount, this.reducePhaseCount);
        System.out.printf("mc=%s, rp=%s => config=%s\n", this.mapperCount, this.reducePhaseCount, graphConfig);
        Assert.assertThat(graphConfig.getReducerCountByPhase(), Matchers.is(this.expectedReducerCountByPhase));
        Assert.assertThat(graphConfig.getMapperCountPerPhaseOneReducer(), Matchers.is(this.expectedMapperCountPerPhaseOneReducer));
    }
}