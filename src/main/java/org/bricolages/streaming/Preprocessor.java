package org.bricolages.streaming;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.zip.GZIPOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import lombok.*;
import lombok.extern.java.Log;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Slf4j
public class Preprocessor {
    static public void main(String[] args) throws Exception {
        val config = Config.load(args[0]);
        dumpConfig(config);
        build(config).run();
    }

    static Preprocessor build(Config config) {
        AWSCredentialsProvider credentials = new ProfileCredentialsProvider();
        SQSQueue sqs = new SQSQueue(credentials, config.queue.url);
        sqs.maxNumberOfMessages = 3;
        return new Preprocessor(
            new EventQueue(sqs),
            new S3Agent(credentials),
            new ObjectMapper(config.mapping),
            new ObjectFilter()
        );
    }

    static void dumpConfig(Config config) {
        System.out.println("queue url: " + config.queue.url);
        for (ObjectMapper.Entry map : config.mapping) {
            System.out.println("mapping: " + map.src + " -> " + map.dest);
        }
    }

    final EventQueue eventQueue;
    final S3Agent s3;
    final ObjectMapper mapper;
    final ObjectFilter filter;

    public void run() throws IOException {
        eventQueue.finiteStream().forEach(event -> {
            S3ObjectLocation src = event.getLocation();
            S3ObjectLocation dest = mapper.map(src);
            try {
                FilterResult result = applyFilter(src, dest);
                // FIXME: write to the log table
                log.info("src: {}, dest: {}, in: {}, out: {}", src.urlString(), dest.urlString(), result.inputLines, result.outputLines);
            }
            catch (IOException ex) {   // S3 I/O error
                // FIXME: write to the log table
                log.error("src: {}, error: {}", src.urlString(), ex.getMessage());
            }
        });
    }

    static final String TMPDIR = "/tmp";   // FIXME: parameterize
    static final Charset DATA_FILE_CHARSET = StandardCharsets.UTF_8;

    FilterResult applyFilter(S3ObjectLocation src, S3ObjectLocation dest) throws IOException {
        FilterResult result;
        Path tmp = Paths.get(TMPDIR, dest.basename());
        try (OutputStream s = Files.newOutputStream(tmp)) {
            OutputStream out = dest.isGzip() ? new GZIPOutputStream(s) : s;
            BufferedWriter w = new BufferedWriter(new OutputStreamWriter(out, DATA_FILE_CHARSET));
            try (BufferedReader r = s3.openBufferedReader(src, DATA_FILE_CHARSET)) {
                result = filter.apply(r, w, src.toString());
            }
            w.close();
        }
        s3.upload(tmp, dest);
        return result;
    }
}
