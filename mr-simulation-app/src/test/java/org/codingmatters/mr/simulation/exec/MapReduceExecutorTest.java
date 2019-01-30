package org.codingmatters.mr.simulation.exec;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class MapReduceExecutorTest {

    private File dataSetFile;

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

    @Test
    public void testDataSet() throws Exception {
        try(DataSet data = new DataSet(new FileInputStream(this.dataSetFile))) {
            for (int i = 0; i < 200; i++) {
                Map<String, Object> datum = new HashMap<>();
                datum.put("title", "the " + i);

                assertThat(data.next().get(), is(datum));
            }
        }
    }

    @Test
    public void testExecute() throws Exception {
        try(
                DataSet data = new DataSet(new FileInputStream(this.dataSetFile));
                Reader map = new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream("map.js"));
                Reader reduce = new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream("reduce.js"));
                MapReduceExecutor executor = new MapReduceExecutor(new MapReduceConfig(data, map, reduce))
        ) {
            Map<String, Map<String, Object>> result = executor.execute().get();

            assertThat(result.size(), is(1));
            assertThat(result.get("key").size(), is(1));
            assertThat(result.get("key").get("cnt"), is(200.0));
        }
    }

    @After
    public void tearDown() throws Exception {
        this.dataSetFile.delete();
    }
}