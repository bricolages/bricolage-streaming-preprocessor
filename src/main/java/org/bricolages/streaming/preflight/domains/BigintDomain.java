package org.bricolages.streaming.preflight.domains;
import org.bricolages.streaming.preflight.definition.ColumnEncoding;
import org.bricolages.streaming.preflight.definition.OperatorDefinitionEntry;
import org.bricolages.streaming.preflight.ReferenceGenerator.MultilineDescription;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.*;

@JsonTypeName("bigint")
@MultilineDescription("64bit signed integral number")
public class BigintDomain extends PrimitiveDomain {
    @Getter private final String type = "bigint";
    @Getter private final ColumnEncoding encoding = ColumnEncoding.ZSTD;

    // This is necessary to accept null value
    @JsonCreator public BigintDomain(String nil) { /* noop */ }
}
