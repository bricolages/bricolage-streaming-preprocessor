package org.bricolages.streaming.preflight;

import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.regex.Pattern;


import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.*;

class StreamDefinitionEntry {
    @Getter private String schemaName;
    @Getter private String tableName;
    @Getter private List<ColumnDefinitionEntry> columns;

    static StreamDefinitionEntry load(Reader yamlSource) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
            .setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE)
            .configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
        return mapper.readValue(yamlSource, StreamDefinitionEntry.class);
    }

    static final Pattern schemaPattern = Pattern.compile("^\\$?[a-z_][a-z0-9_]*$");
    static final Pattern tablePattern = Pattern.compile("^[a-z_][a-z0-9_]*$");

    public void setSchemaName(String schemaName) {
        if (!schemaPattern.matcher(schemaName).matches()) {
            throw new IllegalArgumentException("schema name is invalid: " + schemaName);
        }
        this.schemaName = schemaName;
    }

    public void setTableName(String tableName) {
        if (!tablePattern.matcher(tableName).matches()) {
            throw new IllegalArgumentException("table name is invalid: " + tableName);
        }
        this.tableName = tableName;
    }

    public String getFullTableName() {
        return getSchemaName() + "." + getTableName();
    }
}
