package org.codingmatters.mr.simulation.exec;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.codingmatters.mr.simulation.exec.data.set.StreamDataSet;
import org.codingmatters.mr.simulation.io.FunctionSupplier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.*;
import java.util.*;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class MapReduceExecutorTest {


    @Parameterized.Parameters(name = "mapper count : {0}")
    public static Collection mapperCount() {
        return Arrays.asList(
                new Object[] {1},
                new Object[] {2},
                new Object[] {5},
                new Object[] {10}
        );
    }

    private final int mapperCount;

    private File dataSetFile;

    public MapReduceExecutorTest(int mapperCount) {
        this.mapperCount = mapperCount;
    }

    @Before
    public void setUp() throws Exception {
        this.dataSetFile = File.createTempFile("data-set", ".json");
        this.dataSetFile.deleteOnExit();

        ObjectMapper mapper = new ObjectMapper();
        try(OutputStream out = new FileOutputStream(this.dataSetFile)) {
            for (int i = 0; i < 200; i++) {
                Map<String, Object> datum = new HashMap<>();
                datum.put("title", "the " + i);
                out.write((mapper.writeValueAsString(datum) + "\n").getBytes());
                out.flush();
            }
        }
    }

    @After
    public void tearDown() throws Exception {
        this.dataSetFile.delete();
    }

    @Test
    public void testExecute() throws Exception {
        FunctionSupplier map = () -> new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream("map.js"));
        FunctionSupplier reduce = () -> new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream("reduce.js"));
        try(
                StreamDataSet data = new StreamDataSet(new FileInputStream(this.dataSetFile));
                MapReduceExecutor executor = new MapReduceExecutor(new MapReduceConfig(data, map, reduce, this.mapperCount))
        ) {
            Map<String, Map<String, Object>> result = executor.execute().get();

            assertThat(result.size(), is(1));
            assertThat(result.get("key").size(), is(1));
            assertThat(result.get("key").get("cnt"), is(200.0));
        }
    }
}