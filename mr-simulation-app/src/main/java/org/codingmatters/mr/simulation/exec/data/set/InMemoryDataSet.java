package org.codingmatters.mr.simulation.exec.data.set;

import org.codingmatters.mr.simulation.exec.DataSet;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class InMemoryDataSet implements DataSet {

    private final Iterator<Map<String, Object>> deleguate;

    public InMemoryDataSet(List<Map<String, Object>> values) {
        this.deleguate = values.iterator();
    }

    @Override
    public synchronized Optional<Map<String, Object>> next() throws IOException {
        if(this.deleguate.hasNext()) {
            return Optional.of(this.deleguate.next());
        } else {
            return Optional.empty();
        }
    }
}
