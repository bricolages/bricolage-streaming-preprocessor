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

    public S3ObjectMetadata processLocator(S3ObjectLocator src, S3ObjectLocator dest, PacketFilterLog filterLog) throws ObjectIOException {
        try {
            try (ObjectIOManager.Buffer buf = ioManager.openWriteBuffer(dest)) {
                try (BufferedReader r = ioManager.openBufferedReader(src)) {
                    processStream(r, buf.getBufferedWriter(), filterLog, src.toString());
                }
                return buf.commit();
            }
        }
        catch (UncheckedIOException ex) {
            throw new ObjectIOException(ex.getCause());
        }
        catch (IOException ex) {
            throw new ObjectIOException(ex);
        }
    }


    public PacketFilterLog processLocatorAndPrint(S3ObjectLocator src, BufferedWriter out) throws ObjectIOException {
        try {
            val filterLog = new PacketFilterLog(src.toString(), null);
            try (BufferedReader r = ioManager.openBufferedReader(src)) {
                processStream(r, out, filterLog, src.toString());
            }
            return filterLog;
        }
        catch (UncheckedIOException ex) {
            throw new ObjectIOException(ex.getCause());
        }
        catch (IOException ex) {
            throw new ObjectIOException(ex);
        }
    }

    public void processStream(BufferedReader r, BufferedWriter w, PacketFilterLog filterLog, String sourceName) throws ObjectIOException {
        try {
            final PrintWriter out = new PrintWriter(w);
            r.lines().forEach((line) -> {
                if (line.trim().isEmpty()) return;  // should not count blank line
                filterLog.inputRows++;
                try {
                    String outStr = processJSON(line, filterLog);
                    if (outStr != null) {
                        out.println(outStr);
                        filterLog.outputRows++;
                    }
                }
                catch (JSONParseException ex) {
                    log.debug("JSON parse error: {}:{}: {}", sourceName, filterLog.inputRows, ex.getMessage());
                    filterLog.errorRows++;
                }
            });
        }
        catch (UncheckedIOException ex) {
            throw new ObjectIOException(ex.getCause());
        }
    }

    public String processJSON(String json, PacketFilterLog filterLog) throws JSONParseException {
        Record record = Record.parse(json);
        if (record == null) return null;
        Record result = processRecord(record, filterLog);
        if (result == null) return null;
        return result.serialize();
    }

    public Record processRecord(Record record, PacketFilterLog filterLog) {
        // I apply (old) ops first, because it may includes record-wise operation such as reject op.
        record = processRecordByOperators(record);
        if (record == null) return null;

        if (useProcessor) {
            record = processRecordByProcessors(record, filterLog);
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

    Record processRecordByProcessors(Record src, PacketFilterLog filterLog) {
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
                filterLog.addUnknownColumn((String)name);
            }
        });
        return dest.isEmpty() ? null : dest;
    }

    Pattern IDENTIFIER_PATTERN = Pattern.compile("^[a-zA-Z0-9_]+$");

    boolean isIdentifier(String name) {
        return IDENTIFIER_PATTERN.matcher(name).matches();
    }
}
