package org.bricolages.streaming.preflight.definition;

import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.*;

public class DomainCollection extends HashMap<String, DomainParameters> {
    public static DomainCollection load(Reader yamlSource) throws IOException {
        DomainResolver resolver = new DomainResolver();
        InjectableValues inject = new InjectableValues.Std().addValue(DomainResolver.class, resolver);
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
            .setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE)
            .configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
        val domainCollection = mapper.setInjectableValues(inject).readValue(yamlSource, DomainCollection.class);
        resolver.setDomainCollection(domainCollection); // set itself to resolver to treat self reference well
        return domainCollection;
    }

    public static DomainCollection empty() {
        return new DomainCollection();
    }

    public DomainResolver getResolver() {
        return new DomainResolver(this);
    }

    @NoArgsConstructor
    @AllArgsConstructor
    public static class DomainResolver {
        @Setter DomainCollection domainCollection;

        public DomainParameters resolve(String domainName) {
            val domain = this.domainCollection.get(domainName);
            if (domain == null) {
                throw new DomainResolutionException(String.format("undefined domain: `%s`", domainName));
            }
            return domain;
        }
    }
}
