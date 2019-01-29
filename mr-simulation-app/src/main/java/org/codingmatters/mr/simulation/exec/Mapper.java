package org.codingmatters.mr.simulation.exec;

import org.codingmatters.mr.simulation.exec.exceptions.MapperException;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.Reader;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Mapper {
    private final ScriptEngine engine = new ScriptEngineManager().getEngineByName("nashorn");
    private final Bindings bindings = engine.createBindings();

    private final Emitter emitter = new Emitter();

    public Mapper(Reader mapFunctionReader) throws MapperException {
        try {
            this.bindings.put("emitter", this.emitter);
            this.engine.eval("function emit(key, value) {emitter.emit(key, value); }", this.bindings);
            this.engine.eval(mapFunctionReader, this.bindings);
        } catch (ScriptException e) {
            throw new MapperException("error parsing map function", e);
        }
    }

    public void map(Map<String, Object> datum) throws MapperException {
        this.bindings.put("datum", datum);
        try {
            this.engine.eval("map(datum);", this.bindings);
        } catch (ScriptException e) {
            throw new MapperException("error mapping datum : " + datum, e);
        }
    }

    public Set<String> emittedKeys() {
        return emitter.emittedKeys();
    }

    public List<Map<String, Object>> emittedFor(String key) {
        return emitter.emittedFor(key);
    }

    @Override
    public String toString() {
        return "Mapper{" +
                "engine=" + engine +
                ", bindings=" + bindings +
                ", emitter=" + emitter +
                '}';
    }
}
