package org.codingmatters.mr.simulation.exec.exceptions;

import javax.script.ScriptException;

public class MapperException extends Exception {
    public MapperException(String s, Throwable throwable) {
        super(s, throwable);
    }
}
