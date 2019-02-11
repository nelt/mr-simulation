package org.codingmatters.mr.simulation.exec;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

public class MapperTest {

    @Test
    public void singleMap() throws Exception {
        Mapper mapper = new Mapper(() -> new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream("map.js")));

        Map<String, Object> datum = new HashMap<>();
        datum.put("title", "yop !");
        mapper.map(datum);

        assertThat(mapper.emittedKeys(), contains("key"));
        assertThat(mapper.emittedFor("key"), hasSize(1));
        assertThat(mapper.emittedFor("key").get(0).get("cnt"), is(1));
    }

    @Test
    public void executionContextSettedUp() throws Exception {
        Mapper mapper = new Mapper(() -> new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream("map-exec-context.js")));

        Map<String, Object> datum = new HashMap<>();
        datum.put("title", "yop !");
        mapper.map(datum);

        assertThat(mapper.emittedKeys(), contains("2019"));
    }
}
