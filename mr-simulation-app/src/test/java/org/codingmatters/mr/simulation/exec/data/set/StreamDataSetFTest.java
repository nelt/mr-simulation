package org.codingmatters.mr.simulation.exec.data.set;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class StreamDataSetFTest {

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

    @After
    public void tearDown() throws Exception {
        this.dataSetFile.delete();
    }

    @Test
    public void testDataSet() throws Exception {
        try(StreamDataSet data = new StreamDataSet(new FileInputStream(this.dataSetFile))) {
            for (int i = 0; i < 200; i++) {
                Map<String, Object> datum = new HashMap<>();
                datum.put("title", "the " + i);

                assertThat(data.next().get(), is(datum));
            }
        }
    }

    @Test
    public void testDataSetInParallelContext() throws Exception {
        try(StreamDataSet data = new StreamDataSet(new FileInputStream(this.dataSetFile))) {
            AtomicInteger count = new AtomicInteger(0);
            ExecutorService pool = Executors.newFixedThreadPool(5);

            List<Callable<Integer>> callables = new LinkedList<>();
            for (int i = 0; i < 5; i++) {
                callables.add(() -> {
                    int result = 0;
                    Optional<Map<String, Object>> datum;
                    do {
                        datum = data.next();
                        if (datum.isPresent()) {
                            count.incrementAndGet();
                            result++;
                        }
                    } while (datum.isPresent());
                    return result;
                });
            }

            int cumulatedCount = 0;
            List<Future<Integer>> futures = pool.invokeAll(callables);
            for (Future<Integer> future : futures) {
                cumulatedCount += future.get();
            }

            assertThat("cumulated count", cumulatedCount, is(200));
            assertThat("getted dataset sount", count.get(), is(200));
        }
    }

}
