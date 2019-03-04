package org.codingmatters.mr.simulation.exec.concentrator;

import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.*;

public class ConcentratorTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void whenNoSourceSupplied__thenInstanciationFails() throws Exception {
        this.thrown.expect(IllegalArgumentException.class);
        new Concentrator(stringListMap -> {}, 0);
    }

    @Test
    public void givenConcentratingOneSource__whenTakeCalledOnce__thenValuesAreConcentrated() throws Exception {
        AtomicReference concentrated = new AtomicReference();

        new Concentrator(stringListMap -> concentrated.set(stringListMap), 1).take(this.buildSomeValues());

        assertThat(concentrated.get(), is(this.buildSomeValues()));
    }

    @Test
    public void givenConcentratingOneSource__whenTakeCalledTwice__thenCallFails() throws Exception {
        this.thrown.expect(IllegalStateException.class);

        new Concentrator(stringListMap -> {}, 1)
                .take(this.buildSomeValues())
                .take(this.buildSomeValues());
    }

    @Test
    public void givenConcentratingTwoSources__whenTakeCalledOnce__thenValuesAreNotYetConcentrated() throws Exception {
        AtomicReference concentrated = new AtomicReference();

        new Concentrator(stringListMap -> concentrated.set(stringListMap), 2).take(this.buildSomeValues());

        assertThat(concentrated.get(), is(nullValue()));
    }

    @Test
    public void givenConcentratingTwoSources__whenTakeCalledTwice__thenValuesAreConcentrated() throws Exception {
        AtomicReference concentrated = new AtomicReference();

        System.out.println(this.merge(this.buildSomeValues(), this.buildSomeValues()));

        new Concentrator(stringListMap -> concentrated.set(stringListMap), 2)
                .take(this.buildSomeValues())
                .take(this.buildSomeValues());

        assertThat(concentrated.get(), is(this.merge(this.buildSomeValues(), this.buildSomeValues())));
    }

    private Map<String, List<Map<String, Object>>> merge(Map<String, List<Map<String, Object>>> values1, Map<String, List<Map<String, Object>>> values2) {
        Map<String, List<Map<String, Object>>> result = new HashMap<>();
        for (String key : values1.keySet()) {
            result.put(key, new LinkedList<>(values1.get(key)));
        }

        for (String key : values2.keySet()) {
            if(! result.containsKey(key)) {
                result.put(key, new LinkedList<>(values2.get(key)));
            } else {
                result.get(key).addAll(values2.get(key));
            }
        }

        return result;
    }


    private Map<String, List<Map<String, Object>>> buildSomeValues() {
        Map<String, List<Map<String, Object>>> values = new HashMap<>();
        List<Map<String, Object>> list = new LinkedList<>();
        Map<String, Object> map = new HashMap<>();
        map.put("p", "v");
        list.add(map);
        values.put("key", list);
        return values;
    }


}