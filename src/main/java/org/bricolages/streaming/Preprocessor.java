package org.bricolages.streaming;
import org.bricolages.streaming.filter.*;
import org.bricolages.streaming.event.*;
import org.bricolages.streaming.s3.*;
import org.springframework.beans.factory.annotation.Autowired;
import java.io.BufferedReader;
import java.io.IOException;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class Preprocessor implements EventHandlers {
    final EventQueue eventQueue;
    final S3Agent s3;
    final ObjectMapper mapper;
    final ObjectFilterFactory filterFactory;

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

    @Autowired
    FilterResultRepository repos;

    @Override
    public void handleS3Event(S3Event event) {
        S3ObjectLocation src = event.getLocation();
        val mapResult = mapper.map(src);
        if (mapResult == null) return;
        S3ObjectLocation dest = mapResult.getDestLocation();
        FilterResult result = new FilterResult(src.urlString(), dest.urlString());
        try {
            repos.save(result);
            ObjectFilter filter = filterFactory.load(mapResult.getTableId());
            applyFilter(filter, src, dest, result);
            log.debug("src: {}, dest: {}, in: {}, out: {}", src.urlString(), dest.urlString(), result.inputRows, result.outputRows);
            result.succeeded();
            repos.save(result);
            eventQueue.delete(event);
        }
        catch (S3IOException | IOException ex) {
            log.error("src: {}, error: {}", src.urlString(), ex.getMessage());
            result.failed(ex.getMessage());
            repos.save(result);
        }
    }

    void applyFilter(ObjectFilter filter, S3ObjectLocation src, S3ObjectLocation dest, FilterResult result) throws S3IOException, IOException {
        try (S3Agent.Buffer buf = s3.openWriteBuffer(dest)) {
            try (BufferedReader r = s3.openBufferedReader(src)) {
                filter.apply(r, buf.getBufferedWriter(), src.toString(), result);
            }
            buf.commit();
        }
    }
}
