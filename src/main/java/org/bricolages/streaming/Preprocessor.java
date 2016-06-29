package org.bricolages.streaming;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.charset.Charset;
import lombok.*;
import lombok.extern.java.Log;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Slf4j
public class Preprocessor {
    static public void main(String[] args) throws Exception {
        String queueUrl = args[0];
        AWSCredentialsProvider credentials = new ProfileCredentialsProvider();
        SQSQueue sqs = new SQSQueue(credentials, queueUrl);
        sqs.maxNumberOfMessages = 1;
        Preprocessor pp = new Preprocessor(
            new EventQueue(sqs),
            new S3Agent(credentials),
            new ObjectMapper(),
            new ObjectFilter()
        );
        pp.run();
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

    FilterResult applyFilter(S3ObjectLocation src, S3ObjectLocation dest) throws IOException {
        FilterResult result;
        // FIXME: write to tmp fs
        try (BufferedWriter w = Files.newBufferedWriter(dest.basename(), Charset.defaultCharset())) {
            try (BufferedReader r = s3.openBufferedReader(src)) {
                result = filter.apply(r, w, src.toString());
            }
        }
        // FIXME: upload to S3
        //uploadFile();
        return result;
    }
}
