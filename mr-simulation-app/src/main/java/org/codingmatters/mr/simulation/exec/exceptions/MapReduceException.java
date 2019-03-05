package org.codingmatters.mr.simulation.exec.exceptions;

import java.util.List;

public class MapReduceException extends Exception {
    public MapReduceException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public MapReduceException(String s, List<MapperException> exceptions) {
        super(
                String.format("%s [%s mapper exceptions where thrown, only linking the first one]", s, exceptions.size()),
                exceptions.get(0));
    }
}
