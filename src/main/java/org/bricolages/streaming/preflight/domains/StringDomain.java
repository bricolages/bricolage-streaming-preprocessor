package org.bricolages.streaming.preflight.domains;
import org.bricolages.streaming.preflight.definition.ColumnEncoding;
import org.bricolages.streaming.preflight.definition.OperatorDefinitionEntry;
import org.bricolages.streaming.preflight.ReferenceGenerator.MultilineDescription;
import org.bricolages.streaming.stream.StreamColumn;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.*;

@JsonTypeName("string")
@MultilineDescription({
    "Text",
    "This provides a shorthand such as `!string [bytes]`",
})
@NoArgsConstructor
public class StringDomain extends PrimitiveDomain {
    @Getter
    @MultilineDescription("Declares max byte length")
    private Integer bytes;

    public String getType() {
        return String.format("varchar(%d)", bytes);
    }

    @Getter private final ColumnEncoding encoding = ColumnEncoding.ZSTD;

    @JsonCreator
    public StringDomain(String bytes) {
        this.bytes = Integer.valueOf(bytes);
    }

    public StreamColumn.Params getStreamColumnParams() {
        val params = super.getStreamColumnParams();
        params.type = "string";
        params.length = this.bytes;
        return params;
    }
}
