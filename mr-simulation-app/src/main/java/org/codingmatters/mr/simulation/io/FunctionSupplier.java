package org.codingmatters.mr.simulation.io;

import java.io.IOException;
import java.io.Reader;

@FunctionalInterface
public interface FunctionSupplier {
    Reader get() throws IOException;
}
