package org.codingmatters.mr.simulation.exec;

import jdk.nashorn.api.scripting.NashornScriptEngineFactory;

import javax.script.ScriptEngine;

public class EngineProvider {

    @SuppressWarnings("removal")
    static NashornScriptEngineFactory factory = new NashornScriptEngineFactory();

    static public ScriptEngine createEngine() {
        return factory.getScriptEngine();
    }
}
