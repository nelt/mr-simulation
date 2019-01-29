package org.codingmatters.mr.simulation.exec;

import jdk.nashorn.api.scripting.ScriptObjectMirror;
import org.codingmatters.mr.simulation.exec.exceptions.MapperException;
import org.codingmatters.mr.simulation.exec.exceptions.ReducerException;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.Reader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Reducer {
    private final ScriptEngine engine = new ScriptEngineManager().getEngineByName("nashorn");
    private final Bindings bindings = engine.createBindings();

    private final Emitter emitter = new Emitter();


    public Reducer(Reader reduceFunctionReader) throws ReducerException {
        try {
            this.engine.eval(reduceFunctionReader, this.bindings);
        } catch (ScriptException e) {
            throw new ReducerException("error parsing reduce function", e);
        }
    }

    public Map<String, Object> reduce(List<Map<String, Object>> values) throws ReducerException {
        this.bindings.put("values", values.toArray());
        try {
            ScriptObjectMirror reduced = (ScriptObjectMirror) this.engine.eval("reduce(values);", this.bindings);
            Map<String, Object> result = new HashMap<>();
            for (String key : reduced.keySet()) {
                result.put(key, reduced.get(key));
            }
            return result;
        } catch (ScriptException e) {
            throw new ReducerException("error reducing value set : " + values, e);
        }
    }
}
