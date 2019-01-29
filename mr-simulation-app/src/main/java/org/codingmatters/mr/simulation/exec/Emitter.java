package org.codingmatters.mr.simulation.exec;

import jdk.nashorn.api.scripting.ScriptObjectMirror;

import java.util.*;

public class Emitter {
    private final Map<String, List<ScriptObjectMirror>> emitted = new HashMap<>();

    public void emit(String key, ScriptObjectMirror value) {
        this.emitted.computeIfAbsent(key, s -> new LinkedList<>());
        this.emitted.get(key).add(value);
    }

    public Set<String> emittedKeys() {
        return this.emitted.keySet();
    }

    public List<Map<String, Object>> emittedFor(String key) {
        List<Map<String, Object>> result = new LinkedList<>();
        if(this.emitted.containsKey(key)) {
            for (ScriptObjectMirror objectMirror : this.emitted.get(key)) {
                Map<String, Object> value = new HashMap<>();
                for (String prop : objectMirror.keySet()) {
                    value.put(prop, objectMirror.get(prop));
                }
                result.add(value);
            }
        }
        return result;
    }

    @Override
    public String toString() {
        return "Emitter{" +
                "emitted=" + emitted +
                '}';
    }
}
