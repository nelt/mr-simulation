package org.codingmatters.mr.simulation.exec;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Optional;

public class DataSet implements AutoCloseable {

    private final InputStream in;
    private final JsonFactory jsonFactory;
    private final JsonParser jsonParser;
    private final ObjectMapper mapper;

    public DataSet(InputStream in) throws IOException {
        this.in = in;
        this.jsonFactory = new JsonFactory();
        this.jsonParser = this.jsonFactory.createParser(this.in);
        this.jsonParser.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        this.mapper = new ObjectMapper();
    }

    @Override
    public void close() throws Exception {
        this.jsonParser.close();
        this.in.close();
    }

    public synchronized Optional<Map<String, Object>> next() throws IOException {
        if(this.jsonParser.nextToken() == JsonToken.START_OBJECT) {
            return Optional.of(this.mapper.readValue(this.jsonParser, Map.class));
        }
        return Optional.empty();
    }
}
