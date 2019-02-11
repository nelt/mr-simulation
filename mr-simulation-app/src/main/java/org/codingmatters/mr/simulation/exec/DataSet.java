package org.codingmatters.mr.simulation.exec;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public interface DataSet {
    Optional<Map<String, Object>> next() throws IOException;
}
