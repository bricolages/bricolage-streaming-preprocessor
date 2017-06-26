package org.bricolages.streaming.preflight;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.bricolages.streaming.preflight.definition.DomainParametersEntry;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeName;

import lombok.*;

@RequiredArgsConstructor
public class ReferenceGenerator {
    public void generate() throws IOException {
        val docBuilder = new StringBuilder();

        val annotation = DomainParametersEntry.class.getAnnotation(JsonSubTypes.class);
        for (val type: annotation.value()) {
            val clazz = type.value();
            val typeNameAnno = clazz.getAnnotation(JsonTypeName.class);
            if (typeNameAnno == null) { continue; }

            docBuilder.append("## `" + typeNameAnno.value() + "` type\n");
            val classDescAnno = clazz.getAnnotation(MultilineDescription.class);
            if (classDescAnno != null) {
                val classDesc = String.join("\n\n", classDescAnno.value());
                docBuilder.append(classDesc + "\n\n");
            }
            val fields = Arrays.stream(clazz.getDeclaredFields()).
                filter(field -> field.getAnnotation(JsonProperty.class) != null || field.getAnnotation(MultilineDescription.class) != null).
                collect(Collectors.toList());
            if (fields.isEmpty()) {
                docBuilder.append("No parameters.\n\n");
                continue;
            }

            for (val field: fields) {
                val fieldTypeName = field.getType().getSimpleName();
                val fieldPropAnnno = field.getAnnotation(JsonProperty.class);
                String fieldName = field.getName().replaceAll("([^_A-Z])([A-Z])", "$1_$2").toLowerCase();
                if (fieldPropAnnno != null) {
                    fieldName = fieldPropAnnno.value();
                }
                docBuilder.append("- `" + fieldName + "`: `" + fieldTypeName + "`\n");
                val fieldDescAnno = field.getAnnotation(MultilineDescription.class);
                if (fieldDescAnno != null) {
                    val fieldDesc = String.join("\n\n", fieldDescAnno.value());
                    docBuilder.append("  - " + fieldDesc + "\n");
                }
            }
            docBuilder.append("\n");
        }

        val file = new File("types.md");
        try (val os = new FileOutputStream(file)) {
            os.write(docBuilder.toString().getBytes());
        }
    }

    @Retention(RetentionPolicy.RUNTIME)
    public static @interface MultilineDescription {
        String[] value();
    }
}
