package org.bricolages.streaming.preflight.definition;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
public class OperatorDefinitionEntry {
    @JsonProperty("op")
    @Getter
    protected String operatorId;

    @JsonDeserialize(using = ObjectTreeDeserializer.class)
    protected Object params;

    public String getParams() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(params);
        } catch(JsonProcessingException ex)  {
            // this json serialization must be succeed
            // because data is from a valid yaml
            throw new RuntimeException(ex);
        }
    }
}

class ObjectTreeDeserializer extends JsonDeserializer<ObjectNode> {
    public ObjectNode deserialize(JsonParser jp, DeserializationContext ctx) throws IOException {
        return jp.getCodec().readTree(jp);
    }
}
