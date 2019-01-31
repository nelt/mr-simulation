package org.codingmatters.mr.simulation.exec;

import jdk.nashorn.api.scripting.ScriptObjectMirror;

import java.util.Map;

public interface MapperListener {
    void mapped(Map<String, Object> datum);
    void emitted(String key, ScriptObjectMirror value);
}
