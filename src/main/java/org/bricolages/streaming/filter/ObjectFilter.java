package org.bricolages.streaming.filter;
import org.bricolages.streaming.stream.processor.StreamColumnProcessor;
import org.bricolages.streaming.locator.*;
import java.util.List;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.PrintWriter;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ObjectFilter {
    final LocatorIOManager ioManager;
    final List<Op> operators;
    final List<StreamColumnProcessor> processors;
    final boolean useProcessor;

    /** For column stream */
    public ObjectFilter(LocatorIOManager ioManager, List<Op> operators, final List<StreamColumnProcessor> processors) {
        this.ioManager = ioManager;
        this.operators = operators;
        this.processors = processors;
        this.useProcessor = true;
    }

    /** For non-column stream */
    public ObjectFilter(LocatorIOManager ioManager, List<Op> operators) {
        this.ioManager = ioManager;
        this.operators = operators;
        this.processors = null;
        this.useProcessor = false;
    }

    public S3ObjectMetadata processLocator(S3ObjectLocator src, S3ObjectLocator dest, FilterResult result, String sourceName) throws LocatorIOException {
        try {
            try (LocatorIOManager.Buffer buf = ioManager.openWriteBuffer(dest, sourceName)) {
                try (BufferedReader r = ioManager.openBufferedReader(src)) {
                    processStream(r, buf.getBufferedWriter(), result, sourceName);
                }
                return buf.commit();
            }
        }
        catch (UncheckedIOException ex) {
            throw new LocatorIOException(ex.getCause());
        }
        catch (IOException ex) {
            throw new LocatorIOException(ex);
        }
    }


    public FilterResult processLocatorAndPrint(S3ObjectLocator src, BufferedWriter out) throws LocatorIOException {
        try {
            val result = new FilterResult(src.toString(), null);
            try (BufferedReader r = ioManager.openBufferedReader(src)) {
                processStream(r, out, result, src.toString());
            }
            return result;
        }
        catch (UncheckedIOException ex) {
            throw new LocatorIOException(ex.getCause());
        }
        catch (IOException ex) {
            throw new LocatorIOException(ex);
        }
    }

    public void processStream(BufferedReader r, BufferedWriter w, FilterResult result, String sourceName) throws LocatorIOException {
        try {
            final PrintWriter out = new PrintWriter(w);
            r.lines().forEach((line) -> {
                if (line.trim().isEmpty()) return;  // should not count blank line
                result.inputRows++;
                try {
                    String outStr = processRecord(line);
                    if (outStr != null) {
                        out.println(outStr);
                        result.outputRows++;
                    }
                }
                catch (JSONException ex) {
                    log.debug("JSON parse error: {}:{}: {}", sourceName, result.inputRows, ex.getMessage());
                    result.errorRows++;
                }
            });
        }
        catch (UncheckedIOException ex) {
            throw new LocatorIOException(ex.getCause());
        }
    }

    public String processRecord(String json) throws JSONException {
        Record record = Record.parse(json);
        if (record == null) return null;

        // I apply (old) ops first, because it may includes record-wise operation such as reject op.
        record = processRecordByOperators(record);
        if (record == null) return null;

        if (useProcessor) {
            record = processRecordByProcessors(record);
            if (record == null) return null;
        }

        return record.serialize();
    }

    Record processRecordByOperators(Record record) {
        for (Op op : operators) {
            record = op.apply(record);
            if (record == null) return null;
        }
        return record;
    }

    Record processRecordByProcessors(Record record) {
return null;
        /*
        for (ColumnProcessor proc : processors) {
            record = proc.process(record);
        }
        record.removeAllNullColumns();
        return record;
        */
    }
}
