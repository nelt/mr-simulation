package org.codingmatters.mr.simulation.exec;

import org.codingmatters.mr.simulation.exec.exceptions.MapperException;
import org.codingmatters.mr.simulation.io.FunctionSupplier;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.IOException;
import java.io.Reader;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public class Mapper {
    private final ScriptEngine engine = EngineProvider.createEngine();
    private final Bindings bindings = engine.createBindings();

    private final Emitter emitter = new Emitter();

    public Mapper(FunctionSupplier mapFunctionReader) throws MapperException {
        try(Reader reader = mapFunctionReader.get()) {
            ExecutionContext.setup(this.bindings);
            this.bindings.put("emitter", this.emitter);
            this.engine.eval("function emit(key, value) {emitter.emit(key, value); }", this.bindings);
            this.engine.eval(reader, this.bindings);
        } catch (ScriptException | IOException e) {
            throw new MapperException("error parsing map function", e);
        }
    }

    private final HashSet<MapperListener> listeners = new HashSet<>();

    public void register(MapperListener listener) {
        this.listeners.add(listener);
        this.emitter.register(listener);
    }

    public void map(Map<String, Object> datum) throws MapperException {
        this.bindings.put("datum", datum);
        try {
            this.engine.eval("map(datum);", this.bindings);
            this.listeners.forEach(listener -> listener.mapped(datum));
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
