package org.codingmatters.mr.simulation.exec;

import jdk.nashorn.api.scripting.ScriptObjectMirror;
import org.codingmatters.mr.simulation.exec.exceptions.MapperException;
import org.codingmatters.mr.simulation.exec.exceptions.ReducerException;
import org.codingmatters.mr.simulation.io.FunctionSupplier;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class Reducer {
    private final ScriptEngine engine = EngineProvider.createEngine();
    private final Bindings bindings = engine.createBindings();

    public Reducer(FunctionSupplier reduceFunctionReader) throws ReducerException {
        try (Reader reader = reduceFunctionReader.get()) {
            ExecutionContext.setup(this.bindings);
            this.engine.eval(reader, this.bindings);
        } catch (ScriptException | IOException e) {
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
            throw new ReducerException("error reducing value set : " + this.bindings.get("values"), e);
        }
    }
}
