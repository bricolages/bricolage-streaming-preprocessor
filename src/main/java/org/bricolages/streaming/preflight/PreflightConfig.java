package org.bricolages.streaming.preflight;

import java.io.IOException;
import java.io.Reader;
import org.bricolages.streaming.preflight.domains.DomainDefaultValues;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import lombok.*;

public class PreflightConfig {
    @Getter private DomainDefaultValues defaultValues;

    public static PreflightConfig load(Reader yamlSource) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
            .setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE)
            .configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
        return mapper.readValue(yamlSource, PreflightConfig.class);
    }
}
