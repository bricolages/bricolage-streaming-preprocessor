package org.bricolages.streaming.preflight.domains;
import org.bricolages.streaming.preflight.definition.ColumnEncoding;
import org.bricolages.streaming.preflight.definition.OperatorDefinitionEntry;
import org.bricolages.streaming.preflight.ReferenceGenerator.MultilineDescription;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.*;

@JsonTypeName("real")
@MultilineDescription("32bit floating point number")
public class RealDomain extends PrimitiveDomain {
    @Getter private final String type = "real";
    @Getter private final ColumnEncoding encoding = ColumnEncoding.ZSTD;

    // This is necessary to accept null value
    @JsonCreator public RealDomain(String nil) { /* noop */ }
}
