package org.bricolages.streaming.preflight;

import java.io.IOException;
import java.io.Reader;
import java.util.HashSet;
import java.util.List;
import org.bricolages.streaming.preflight.DomainCollection.DomainResolver;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.*;

class StreamDefinitionEntry {
    @Getter private List<ColumnParametersEntry> columns;

    void validate() {
        val names = new HashSet<String>();
        for (ColumnParametersEntry columnDef: columns) {
            val name = columnDef.getName();
            if (name == null) {
                throw new RuntimeException("name is null"); // FIXME
            }
            if (names.contains(name)) {
                throw new RuntimeException(String.format("duplicated domain name: `%s`", name));
            }
            names.add(name);
        }
    }

    static StreamDefinitionEntry load(Reader yamlSource, DomainCollection domains) throws IOException {
        InjectableValues inject = new InjectableValues.Std().addValue(DomainResolver.class, domains.getResolver());
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
            .setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE)
            .configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
        return mapper.setInjectableValues(inject).readValue(yamlSource, StreamDefinitionEntry.class);
    }
}
