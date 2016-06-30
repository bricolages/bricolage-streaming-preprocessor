package org.bricolages.streaming;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import java.io.BufferedReader;
import java.io.IOException;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class Preprocessor implements EventHandlers {
    static public void main(String[] args) throws Exception {
        val config = Config.load(args[0]);
        dumpConfig(config);
        build(config).handleEvents();
        //build(config).run();
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
        log.info("server started");
        trapSignals();
        try {
            while (!isTerminating()) {
                // FIXME: insert sleep on empty result
                try {
                    handleEvents();
                }
                catch (SQSException ex) {
                    safeSleep(5);
                }
            }
        }
        catch (ApplicationAbort ex) {
            // ignore
        }
        log.info("application is gracefully shut down");
    }

    void handleEvents() {
        eventQueue.stream().forEach(event -> {
            log.debug("processing message: {}", event.getMessageBody());
            event.callHandler(this);
        });
    }

    @Override
    public void handleUnknownEvent(UnknownEvent event) {
        // FIXME: notify?
        log.warn("unknown message: {}", event.getMessageBody());
        // Keep message in the queue.
    }

    @Override
    public void handleShutdownEvent(ShutdownEvent event) {
        eventQueue.delete(event);
        initiateShutdown();
    }

    Thread mainThread;
    boolean isTerminating = false;

    void trapSignals() {
        mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                initiateShutdown();
                waitMainThread();
            }
        });
    }

    void initiateShutdown() {
        log.info("initiate shutdown; mainThread={}", mainThread);
        this.isTerminating = true;
        if (mainThread != null) {
            mainThread.interrupt();
        }
    }

    boolean isTerminating() {
        if (isTerminating) return true;
        if (mainThread.isInterrupted()) {
            this.isTerminating = true;
            return true;
        }
        else {
            return false;
        }
    }

    void waitMainThread() {
        if (mainThread == null) return;
        try {
            log.info("waiting main thread...");
            mainThread.join();
        }
        catch (InterruptedException ex) {
            // ignore
        }
    }

    void safeSleep(int sec) {
        try {
            Thread.sleep(sec * 1000);
        }
        catch (InterruptedException ex) {
            this.isTerminating = true;
        }
    }

    @Override
    public void handleS3Event(S3Event event) {
        S3ObjectLocation src = event.getLocation();
        S3ObjectLocation dest = mapper.map(src);
        try {
            FilterResult result = applyFilter(src, dest);
            // FIXME: write to the log table
            log.info("src: {}, dest: {}, in: {}, out: {}", src.urlString(), dest.urlString(), result.inputLines, result.outputLines);
            eventQueue.delete(event);
        }
        catch (S3IOException ex) {
            // FIXME: write to the log table
            log.error("src: {}, error: {}", src.urlString(), ex.getMessage());
        }
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
