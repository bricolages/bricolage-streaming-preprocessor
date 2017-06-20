package org.bricolages.streaming.preflight.types;

import java.util.ArrayList;
import java.util.List;
import org.bricolages.streaming.preflight.ColumnEncoding;
import org.bricolages.streaming.preflight.OperatorDefinitionEntry;
import org.bricolages.streaming.preflight.ReferenceGenerator.MultilineDescription;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.*;

@JsonTypeName("string")
@MultilineDescription({
    "Text",
    "This provides a shorthand such as `!string [bytes]`",
})
@NoArgsConstructor
public class StringType extends PrimitiveType {
    @Getter
    @MultilineDescription("Declares max byte length")
    private Integer bytes;

    public String getType() {
        return String.format("varchar(%d)", bytes);
    }
    @Getter private final ColumnEncoding encoding = ColumnEncoding.ZSTD;

    public List<OperatorDefinitionEntry> getOperatorDefinitionEntries() {
        val list = new ArrayList<OperatorDefinitionEntry>();
        return list; // empty list
    }

    @JsonCreator
    public StringType(String bytes) {
        this.bytes = Integer.valueOf(bytes);
    }
}
