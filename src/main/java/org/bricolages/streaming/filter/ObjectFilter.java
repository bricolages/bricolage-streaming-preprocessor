package org.bricolages.streaming.filter;
import org.bricolages.streaming.locator.*;
import java.util.List;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.PrintWriter;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class ObjectFilter {
    final LocatorIOManager ioManager;
    final List<Op> operators;

    public S3ObjectMetadata processLocator(S3ObjectLocator src, S3ObjectLocator dest, FilterResult result, String sourceName) throws LocatorIOException, IOException {
        try (LocatorIOManager.Buffer buf = ioManager.openWriteBuffer(dest, sourceName)) {
            try (BufferedReader r = ioManager.openBufferedReader(src)) {
                processStream(r, buf.getBufferedWriter(), result, sourceName);
            }
            return buf.commit();
        }
    }


    public FilterResult processLocatorAndPrint(S3ObjectLocator src, BufferedWriter out) throws LocatorIOException, IOException {
        val result = new FilterResult(src.toString(), null);
        try (BufferedReader r = ioManager.openBufferedReader(src)) {
            processStream(r, out, result, src.toString());
        }
        return result;
    }

    public void processStream(BufferedReader r, BufferedWriter w, FilterResult result, String sourceName) throws IOException {
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

    public String processRecord(String json) throws JSONException {
        Record record = Record.parse(json);
        if (record == null) return null;
        for (Op op : operators) {
            record = op.apply(record);
            if (record == null) return null;
        }
        return record.serialize();
    }
}
