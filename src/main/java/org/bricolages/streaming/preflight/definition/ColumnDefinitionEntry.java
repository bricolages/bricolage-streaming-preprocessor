package org.bricolages.streaming.preflight.definition;
import org.bricolages.streaming.exception.*;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonCreator;
import lombok.*;

@RequiredArgsConstructor
public class ColumnDefinitionEntry implements ColumnDefinition {
    @Getter private final String name;
    @Getter private final String originalName;
    private final DomainParameters domain;

    @JsonCreator
    ColumnDefinitionEntry(Map.Entry<String, DomainParameters> kv) {
        val columnKey = kv.getKey();

        val mapping = columnKey.split("->");
        switch (mapping.length) {
        case 1:
            // pattern: "name: ..."
            this.name = mapping[0].trim();
            this.originalName = null;
            break;
        case 2:
            // pattern: "original_name -> name: ..."
            this.name = mapping[1].trim();
            this.originalName = mapping[0].trim();
            break;
        default:
            throw new BadColumnKeyException(columnKey);
        }
        this.domain = kv.getValue();
    }

    public DomainParameters getDomain() {
        if (this.domain == null) {
            throw new DomainResolutionException("could not find domain: " + this.name);
        }
        return this.domain;
    }

    public class BadColumnKeyException extends ApplicationError {
        BadColumnKeyException(String columnKey) {
            super(String.format("bad column name: %s", columnKey));
        }
    }
}
