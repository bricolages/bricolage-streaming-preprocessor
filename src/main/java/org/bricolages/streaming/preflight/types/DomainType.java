package org.bricolages.streaming.preflight.types;

import java.util.List;
import org.bricolages.streaming.preflight.ColumnEncoding;
import org.bricolages.streaming.preflight.ColumnParametersEntry;
import org.bricolages.streaming.preflight.OperatorDefinitionEntry;
import org.bricolages.streaming.preflight.DomainCollection.DomainResolver;
import org.bricolages.streaming.preflight.ReferenceGenerator.MultilineDescription;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("domain")
@MultilineDescription({
    "Domain"
})
public class DomainType implements ColumnParametersEntry {
    private String domainName;
    private DomainResolver resolver;
    
    private ColumnParametersEntry getDomain() {
        return resolver.resolve(domainName);
    }

    public String getName() {
        return getDomain().getName();
    }

    public String getType() {
        return getDomain().getType();
    }

    public ColumnEncoding getEncoding() {
      return getDomain().getEncoding();
    }

    public String getOriginalName() {
        return getDomain().getOriginalName();
    }

    public List<OperatorDefinitionEntry> getOperatorDefinitionEntries(String columnName) {
        return getDomain().getOperatorDefinitionEntries(columnName);
    }

    @JsonCreator
    public DomainType(String domainName, @JacksonInject DomainResolver resolver) {
        this.domainName = domainName;
        this.resolver = resolver;
    }
}

