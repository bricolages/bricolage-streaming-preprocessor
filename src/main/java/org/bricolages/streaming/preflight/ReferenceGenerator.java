package org.bricolages.streaming.preflight;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;
import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.*;

@RequiredArgsConstructor
public class ReferenceGenerator {
    public void generate() throws IOException {
        val docBuilder = new StringBuilder();

        val annotation = ColumnParametersEntry.class.getAnnotation(JsonSubTypes.class);
        for (val type: annotation.value()) {
            val clazz = type.value();
            val typeNameAnno = clazz.getAnnotation(JsonTypeName.class);
            if (typeNameAnno == null) { continue; }

            docBuilder.append("## `" + typeNameAnno.value() + "` domain\n");
            val classDescAnno = clazz.getAnnotation(JsonClassDescription.class);
            if (classDescAnno != null) {
                docBuilder.append(classDescAnno.value() + "\n\n");
            }
            val fields = Arrays.stream(clazz.getDeclaredFields()).
                filter(field -> field.getAnnotation(JsonProperty.class) != null).
                collect(Collectors.toList());
            if (fields.isEmpty()) {
                docBuilder.append("No parameters.\n\n");
                continue;
            }

            for (val field: fields) {
                val fieldTypeName = field.getType().getSimpleName();
                docBuilder.append("- `" + field.getName() + "`: `" + fieldTypeName + "`\n");
                val fieldDescAnno = field.getAnnotation(JsonPropertyDescription.class);
                if (fieldDescAnno != null) {
                    docBuilder.append("  - " + fieldDescAnno.value() + "\n");
                }
            }
            docBuilder.append("\n");
        }

        val file = new File("domains.md");
        try (val os = new FileOutputStream(file)) {
            os.write(docBuilder.toString().getBytes());
        }
    }
}
