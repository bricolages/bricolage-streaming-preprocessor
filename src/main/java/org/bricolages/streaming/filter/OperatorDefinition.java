package org.bricolages.streaming.filter;
import org.bricolages.streaming.ConfigError;
import javax.persistence.*;
import java.util.List;
import java.io.IOException;
import lombok.*;

@NoArgsConstructor
@AllArgsConstructor
@ToString
@Entity
@Table(name="preproc_definition")
public class OperatorDefinition {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    long id;

    @Column(name="operator_id")
    @Getter
    String operatorId;

    @Column(name="target_table")
    @Getter
    String targetTable;

    @Column(name="target_column")
    String targetColumn;

    @Column(name="params")
    String params;  // JSON string

    public boolean isSingleColumn() {
        return ! this.targetColumn.equals("*");
    }

    public String getTargetColumn() {
        if (!isSingleColumn()) throw new ConfigError("is not a single column op: " + targetTable + ", " + operatorId);
        return targetColumn;
    }

    public <T> T mapParameters(Class<T> type) {
        try {
            val map = new com.fasterxml.jackson.databind.ObjectMapper();
            return map.readValue(params, type);
        }
        catch (IOException err) {
            throw new ConfigError("could not map filter parameters: " + targetTable + "." + targetColumn + ": " + params);
        }
    }
}
