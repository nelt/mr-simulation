package org.codingmatters.mr.simulation.graph;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.codingmatters.mr.simulation.graph.Node.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class MapReduceGraphConfigGraphTest {

    @Parameterized.Parameters(name = "given {0} mappers, when {1} reduce phases, then expecting graph: {2}")
    public static Collection parameters() {
        return Arrays.asList(
                new Object[] {4, 2, last()
                        .withPrevious(reducer()
                                .withPrevious(mapper())
                                .withPrevious(mapper())
                                .build())
                        .withPrevious(reducer()
                                .withPrevious(mapper())
                                .withPrevious(mapper())
                                .build())
                        .build()},
                new Object[] {5, 2, last()
                        .withPrevious(reducer()
                                .withPrevious(mapper())
                                .withPrevious(mapper())
                                .withPrevious(mapper())
                                .build())
                        .withPrevious(reducer()
                                .withPrevious(mapper())
                                .withPrevious(mapper())
                                .build())
                        .build()},
                new Object[] {11, 2, last()
                        .withPrevious(reducer()
                                .withPrevious(mapper())
                                .withPrevious(mapper())
                                .withPrevious(mapper())
                                .withPrevious(mapper())
                                .withPrevious(mapper())
                                .withPrevious(mapper())
                                .build())
                        .withPrevious(reducer()
                                .withPrevious(mapper())
                                .withPrevious(mapper())
                                .withPrevious(mapper())
                                .withPrevious(mapper())
                                .withPrevious(mapper())
                                .build())
                        .build()},
                new Object[] {4, 3, last()
                        .withPrevious(reducer()
                                .withPrevious(reducer()
                                        .withPrevious(mapper())
                                        .withPrevious(mapper())
                                        .withPrevious(mapper())
                                        .withPrevious(mapper())
                                        .build())
                                .build())
                        .build()},
                new Object[] {11, 3, last()
                        .withPrevious(reducer()
                                .withPrevious(reducer()
                                        .withPrevious(mapper())
                                        .withPrevious(mapper())
                                        .build())
                                .withPrevious(reducer()
                                        .withPrevious(mapper())
                                        .withPrevious(mapper())
                                        .build())
                                .withPrevious(reducer()
                                        .withPrevious(mapper())
                                        .build())
                                .build())
                        .withPrevious(reducer()
                                .withPrevious(reducer()
                                        .withPrevious(mapper())
                                        .build())
                                .withPrevious(reducer()
                                        .withPrevious(mapper())
                                        .build())
                                .withPrevious(reducer()
                                        .withPrevious(mapper())
                                        .build())
                                .build())
                        .withPrevious(reducer()
                                .withPrevious(reducer()
                                        .withPrevious(mapper())
                                        .build())
                                .withPrevious(reducer()
                                        .withPrevious(mapper())
                                        .build())
                                .withPrevious(reducer()
                                        .withPrevious(mapper())
                                        .build())
                                .build())
                        .build()},
                new Object[] {16, 3, last()
                        .withPrevious(reducer()
                                .withPrevious(reducer()
                                        .withPrevious(mapper())
                                        .withPrevious(mapper())
                                        .build())
                                .withPrevious(reducer()
                                        .withPrevious(mapper())
                                        .withPrevious(mapper())
                                        .build())
                                .withPrevious(reducer()
                                        .withPrevious(mapper())
                                        .withPrevious(mapper())
                                        .build())
                                .build())
                        .withPrevious(reducer()
                                .withPrevious(reducer()
                                        .withPrevious(mapper())
                                        .withPrevious(mapper())
                                        .build())
                                .withPrevious(reducer()
                                        .withPrevious(mapper())
                                        .withPrevious(mapper())
                                        .build())
                                .withPrevious(reducer()
                                        .withPrevious(mapper())
                                        .withPrevious(mapper())
                                        .build())
                                .build())
                        .withPrevious(reducer()
                                .withPrevious(reducer()
                                        .withPrevious(mapper())
                                        .withPrevious(mapper())
                                        .build())
                                .withPrevious(reducer()
                                        .withPrevious(mapper())
                                        .build())
                                .withPrevious(reducer()
                                        .withPrevious(mapper())
                                        .build())
                                .build())
                        .build()},
                new Object[] {3, 3, last()
                        .withPrevious(reducer()
                                .withPrevious(reducer()
                                        .withPrevious(mapper())
                                        .withPrevious(mapper())
                                        .withPrevious(mapper())
                                        .build())
                                .build())
                        .build()}
        );
    }

    private final int mapperCount;
    private final int reducePhaseCount;
    private final Node expectedLastNode;

    public MapReduceGraphConfigGraphTest(int mapperCount, int reducePhaseCount, Node expectedLastNode) {
        this.mapperCount = mapperCount;
        this.reducePhaseCount = reducePhaseCount;
        this.expectedLastNode = expectedLastNode;
    }

    @Test
    public void test() throws Exception {
        MapReduceGraphConfig graphConfig = new MapReduceGraphConfig(this.mapperCount, this.reducePhaseCount);

        assertThat(
                graphConfig.lastNode(),
                is(this.expectedLastNode)
        );
    }
}