package org.bricolages.streaming.preflight.domains;
import org.bricolages.streaming.preflight.definition.ColumnEncoding;
import org.bricolages.streaming.preflight.definition.OperatorDefinitionEntry;
import org.bricolages.streaming.preflight.ReferenceGenerator.MultilineDescription;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.*;

@JsonTypeName("float")
@MultilineDescription("64bit floating point number")
public class FloatDomain extends PrimitiveDomain {
    @Getter private final String type = "float";
    @Getter private final ColumnEncoding encoding = ColumnEncoding.ZSTD;

    // This is necessary to accept null value
    @JsonCreator public FloatDomain(String nil) { /* noop */ }
}
