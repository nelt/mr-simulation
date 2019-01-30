package org.codingmatters.mr.simulation.exec;

import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class ReducerTest {

    @Test
    public void someReduction() throws Exception {
        Reducer reducer;
        try(Reader reduceFunctionReader = new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream("reduce.js"))) {
            reducer = new Reducer(reduceFunctionReader);
        }

        List<Map<String, Object>> values = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
            Map<String, Object> value = new HashMap<>();
            value.put("cnt", 1);
            values.add(value);
        }

        Map<String, Object> result = reducer.reduce(values);

        assertThat(result.get("cnt"), is(10.0));
    }
}