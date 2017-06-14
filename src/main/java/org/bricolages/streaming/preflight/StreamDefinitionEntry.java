package org.bricolages.streaming.preflight;

import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
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
        val names = new HashMap<String, Integer>();
        for (int index = 0; index < columns.size(); index++) {
            ColumnParametersEntry columnDef = columns.get(index);
            String name;
            String type;
            ColumnEncoding encoding;
            try {
                name = columnDef.getName();
                type = columnDef.getType();
                encoding = columnDef.getEncoding();
            } catch (DomainResolutionException ex) {
                throw new StreamDefinitionLoadingException(index, ex.getMessage());
            }

            // validate `name`
            if (name == null) {
                throw new StreamDefinitionLoadingException(index, "domain name is null");
            }
            if (names.containsKey(name)) {
                throw new StreamDefinitionLoadingException(index, String.format("duplicated domain name with column[%d]: `%s`", index, name));
            }
            names.put(name, index);

            // validate `type`
            if (type == null) {
                throw new StreamDefinitionLoadingException(index, name, "type is null");
            }

            // validate `encoding`
            if (encoding == null) {
                throw new StreamDefinitionLoadingException(index, name, "encoding is null");
            }
        }
    }

    static StreamDefinitionEntry load(Reader yamlSource, DomainCollection domains) throws IOException {
        InjectableValues inject = new InjectableValues.Std().addValue(DomainResolver.class, domains.getResolver());
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
            .setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE)
            .configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
        val entry = mapper.setInjectableValues(inject).readValue(yamlSource, StreamDefinitionEntry.class);
        entry.validate();
        return entry;
    }
}
