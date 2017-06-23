package org.bricolages.streaming.preflight.definition;

import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;

import org.bricolages.streaming.preflight.definition.DomainCollection.DomainResolver;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.*;

public class WellknownColumnCollection extends HashMap<String, ColumnDefinitionEntry> {
    public static WellknownColumnCollection load(Reader yamlSource, DomainCollection domains) throws IOException {
        InjectableValues inject = new InjectableValues.Std().addValue(DomainResolver.class, domains.getResolver());
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
            .setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE)
            .configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
        val columnCollection = mapper.setInjectableValues(inject).readValue(yamlSource, WellknownColumnCollection.class);
        return columnCollection;
    }

    public static WellknownColumnCollection empty() {
        return new WellknownColumnCollection();
    }

    public WellknownColumnResolver getResolver() {
        return new WellknownColumnResolver(this);
    }

    @NoArgsConstructor
    @AllArgsConstructor
    public static class WellknownColumnResolver {
        @Setter WellknownColumnCollection collumnCollection;

        public ColumnDefinitionEntry resolve(String columnName) {
            val columnDefinition = this.collumnCollection.get(columnName);
            if (columnDefinition == null) {
                throw new ColumnResolutionException(String.format("undefined wellknown column: `%s`", columnName));
            }

            return columnDefinition;
        }
    }
}
