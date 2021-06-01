package org.bricolages.streaming.stream;
import org.bricolages.streaming.stream.processor.StreamColumnProcessor;
import org.bricolages.streaming.stream.op.Op;
import org.bricolages.streaming.object.*;
import java.util.List;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.PrintWriter;
import java.util.regex.Pattern;
import java.nio.charset.StandardCharsets;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PacketFilter {
    final ObjectIOManager ioManager;
    final List<Op> operators;
    final List<StreamColumnProcessor> processors;
    final boolean useProcessor;

    /** For column stream */
    public PacketFilter(ObjectIOManager ioManager, List<Op> operators, final List<StreamColumnProcessor> processors) {
        this.ioManager = ioManager;
        this.operators = operators;
        this.processors = processors;
        this.useProcessor = true;
    }

    /** For non-column stream */
    public PacketFilter(ObjectIOManager ioManager, List<Op> operators) {
        this.ioManager = ioManager;
        this.operators = operators;
        this.processors = null;
        this.useProcessor = false;
    }

    public PacketFilterResult processLocator(S3ObjectLocator src, S3ObjectLocator dest) throws ObjectIOException {
        try {
            PacketFilterResult result = null;
            try (ObjectIOManager.Buffer buf = ioManager.openWriteBuffer(dest)) {
                try (BufferedReader r = ioManager.openBufferedReader(src)) {
                    result = processStream(r, buf.getBufferedWriter(), src.toString());
                }
                result.setObjectMetadata(buf.commit());
            }
            return result;
        }
        catch (UncheckedIOException ex) {
            throw new ObjectIOException(ex.getCause());
        }
        catch (IOException ex) {
            throw new ObjectIOException(ex);
        }
    }


    public PacketFilterResult processLocatorAndPrint(S3ObjectLocator src, BufferedWriter out) throws ObjectIOException {
        try {
            try (BufferedReader r = ioManager.openBufferedReader(src)) {
                return processStream(r, out, src.toString());
            }
        }
        catch (UncheckedIOException ex) {
            throw new ObjectIOException(ex.getCause());
        }
        catch (IOException ex) {
            throw new ObjectIOException(ex);
        }
    }

    public PacketFilterResult processStream(BufferedReader r, BufferedWriter w, String sourceName) throws ObjectIOException {
        try {
            val result = new PacketFilterResult();
            final PrintWriter out = new PrintWriter(w);
            r.lines().forEach((line) -> {
                if (line.trim().isEmpty()) return;  // should not count blank line
                result.inputRows++;
                try {
                    String outStr = processJSON(line, result);
                    if (outStr != null) {
                        out.println(outStr);
                        result.outputRows++;
                    }
                }
                catch (JSONParseException ex) {
                    log.debug("JSON parse error: {}:{}: {}", sourceName, result.inputRows, ex.getMessage());
                    result.errorRows++;
                }
            });
            return result;
        }
        catch (UncheckedIOException ex) {
            throw new ObjectIOException(ex.getCause());
        }
    }

    static final long REDSHIFT_LOAD_RECORD_LIMIT = 4194304;  // 4MB

    public String processJSON(String json, PacketFilterResult result) throws JSONParseException {
        if (json.getBytes(StandardCharsets.UTF_8).length > REDSHIFT_LOAD_RECORD_LIMIT) return null;
        Record record = Record.parse(json);
        if (record == null) return null;
        Record outRecord = processRecord(record, result);
        if (outRecord == null) return null;
        return outRecord.serialize();
    }

    public Record processRecord(Record record, PacketFilterResult result) {
        // I apply (old) ops first, because it may includes record-wise operation such as reject op.
        record = processRecordByOperators(record);
        if (record == null) return null;

        if (useProcessor) {
            record = processRecordByProcessors(record, result);
        }

        return record;
    }

    Record processRecordByOperators(Record record) {
        for (Op op : operators) {
            record = op.apply(record);
            if (record == null) return null;
        }
        return record;
    }

    Record processRecordByProcessors(Record src, PacketFilterResult result) {
        Record dest = new Record();
        for (StreamColumnProcessor proc : processors) {
            Object val = proc.process(src);
            if (val != null) {
                dest.put(proc.getDestName(), val);
            }
        }
        src.unconsumedEntries().forEach(ent -> {
            val name = ent.getKey();
            val value = ent.getValue();
            if (value != null) {
                // Do not overwrite
                if (!dest.hasColumn(name)) {
                    dest.put(name, value);
                }
            }
            if (name instanceof String && isIdentifier((String)name) && value != null) {
                result.addUnknownColumn((String)name);
            }
        });
        return dest.isEmpty() ? null : dest;
    }

    Pattern IDENTIFIER_PATTERN = Pattern.compile("^[a-zA-Z0-9_]+$");

    boolean isIdentifier(String name) {
        return IDENTIFIER_PATTERN.matcher(name).matches();
    }
}
