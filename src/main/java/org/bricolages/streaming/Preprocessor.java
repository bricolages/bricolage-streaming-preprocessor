package org.bricolages.streaming;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import java.io.BufferedReader;
import java.io.IOException;
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
            log.debug("processing message: {}", event.getMessageBody());
            S3ObjectLocation src = event.getLocation();
            S3ObjectLocation dest = mapper.map(src);
            try {
                FilterResult result = applyFilter(src, dest);
                // FIXME: write to the log table
                log.info("src: {}, dest: {}, in: {}, out: {}", src.urlString(), dest.urlString(), result.inputLines, result.outputLines);
            }
            catch (S3IOException ex) {   // S3 I/O error
                // FIXME: write to the log table
                log.error("src: {}, error: {}", src.urlString(), ex.getMessage());
            }
        });
    }

    FilterResult applyFilter(S3ObjectLocation src, S3ObjectLocation dest) throws S3IOException {
        try {
            FilterResult result;
            try (S3Agent.Buffer buf = s3.openWriteBuffer(dest)) {
                try (BufferedReader r = s3.openBufferedReader(src)) {
                    result = filter.apply(r, buf.getBufferedWriter(), src.toString());
                }
                buf.commit();
            }
            return result;
        }
        catch (IOException ex) {
            throw new S3IOException("I/O error: " + ex.getMessage());
        }
    }
}
