package org.codingmatters.mr.simulation.exec;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Test;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class DataSetTest {

    @Test
    public void empty() throws Exception {
        try(DataSet dataSet = new DataSet(this.resourceStream("data-set/empty.json"))) {
            assertFalse(dataSet.next().isPresent());
        }
    }

    @Test
    public void oneElement() throws Exception {
        try(DataSet dataSet = new DataSet(this.resourceStream("data-set/one-element.json"))) {
            assertThat(dataSet.next().get(), is(map().put("str", "hello").put("num", 12)));
            assertFalse(dataSet.next().isPresent());
        }
    }

    @Test
    public void manyElement() throws Exception {
        try(DataSet dataSet = new DataSet(this.resourceStream("data-set/many-elements.json"))) {
            assertTrue(dataSet.next().isPresent());
            assertTrue(dataSet.next().isPresent());
            assertTrue(dataSet.next().isPresent());
            assertTrue(dataSet.next().isPresent());


            assertFalse(dataSet.next().isPresent());
        }
    }

    @Test
    public void relaxSyntax() throws Exception {
        try(DataSet dataSet = new DataSet(this.resourceStream("data-set/relaxed-syntax.json"))) {
            assertThat(dataSet.next().get(), is(map().put("str", "hello").put("num", 12)));
            assertFalse(dataSet.next().isPresent());
        }
    }

    private MapMatcher map() {
        return new MapMatcher();
    }

    class MapMatcher extends BaseMatcher<Map<String, Object>> {

        private final Map<String, Object> expected = new HashMap<>();

        public MapMatcher put(String key, Object value) {
            this.expected.put(key, value);
            return this;
        }

        @Override
        public boolean matches(Object o) {
            return this.expected.equals(o);
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("map ").appendValue(this.expected);
        }
    }

    private InputStream resourceStream(String resource) {
        return Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
    }


}