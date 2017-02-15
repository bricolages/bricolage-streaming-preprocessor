package org.bricolages.streaming;
import org.bricolages.streaming.filter.*;
import org.bricolages.streaming.event.*;
import org.bricolages.streaming.s3.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class Preprocessor implements EventHandlers {
    final EventQueue eventQueue;
    final LogQueue logQueue;
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
                    eventQueue.flushDelete();
                }
                catch (SQSException ex) {
                    safeSleep(5);
                }
            }
        }
        catch (ApplicationAbort ex) {
            // ignore
        }
        eventQueue.flushDeleteForce();
        log.info("application is gracefully shut down");
    }

    public void runOneshot() throws Exception {
        trapSignals();
        try {
            while (!isTerminating()) {
                val empty = handleEvents();
                if (empty) break;
            }
        }
        catch (ApplicationAbort ex) {
            // ignore
        }
        eventQueue.flushDeleteForce();
    }

    public boolean processUrl(S3ObjectLocation src, BufferedWriter out) {
        val mapResult = mapper.map(src);
        if (mapResult == null) {
            log.warn("S3 object could not mapped: {}", src);
            return false;
        }
        String streamName = mapResult.getStreamName();
        S3ObjectLocation dest = mapResult.getDestLocation();

        FilterResult result = new FilterResult(src.urlString(), dest.urlString());
        try {
            ObjectFilter filter = filterFactory.load(streamName);
            try (BufferedReader r = s3.openBufferedReader(src)) {
                filter.apply(r, out, src.toString(), result);
            }
            log.debug("src: {}, dest: {}, in: {}, out: {}", src.urlString(), dest.urlString(), result.inputRows, result.outputRows);
            return true;
        }
        catch (S3IOException | IOException ex) {
            log.error("src: {}, error: {}", src.urlString(), ex.getMessage());
            return false;
        }
    }

    boolean handleEvents() {
        boolean empty = true;
        for (val event : eventQueue.poll()) {
            log.debug("processing message: {}", event.getMessageBody());
            event.callHandler(this);
            empty = false;
        }
        return empty;
    }

    @Override
    public void handleUnknownEvent(UnknownEvent event) {
        // FIXME: notify?
        log.warn("unknown message: {}", event.getMessageBody());
        eventQueue.deleteAsync(event);
    }

    @Override
    public void handleShutdownEvent(ShutdownEvent event) {
        // Use sync delete to avoid duplicated shutdown
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

    @Autowired
    StreamParamsRepository paramsRepos;

    @Autowired
    IncomingStreamRepository strRepos;

    @Override
    public void handleS3Event(S3Event event) {
        log.debug("handling URL: {}", event.getLocation().toString());
        S3ObjectLocation src = event.getLocation();
        val mapResult = mapper.map(src);
        if (mapResult == null) {
            log.warn("S3 object could not mapped: {}", src);
            return;
        }
        String streamName = mapResult.getStreamName();
        S3ObjectLocation dest = mapResult.getDestLocation();

        StreamParams params = paramsRepos.findParams(streamName);
        if (params == null) {
            IncomingStream stream = strRepos.findStream(streamName);
            if (stream == null) {
                try {
                    stream = new IncomingStream(streamName);
                    stream = strRepos.save(stream);
                    log.warn("new stream: stream_id={}, stream_name={}", stream.getId(), streamName);
                }
                catch (DataIntegrityViolationException ex) {
                    stream = strRepos.findStream(streamName);
                }
            }
            log.info("new data packet for unconfigured stream: stream_id={}, stream_name={}, url={}", stream.getId(), streamName, src);
            return;
        }
        if (params.isDisabled()) {
            // Processing is temporary disabled; process objects later
            return;
        }
        if (params.doesDiscard()) {
            // Just ignore without processing, do not keep SQS messages.
            log.info("discard event: {}", event.getLocation().toString());
            eventQueue.deleteAsync(event);
            return;
        }

        FilterResult result = new FilterResult(src.urlString(), dest.urlString());
        try {
            repos.save(result);
            ObjectFilter filter = filterFactory.load(streamName);
            S3ObjectMetadata obj = applyFilter(filter, src, dest, result, streamName);
            log.debug("src: {}, dest: {}, in: {}, out: {}", src.urlString(), dest.urlString(), result.inputRows, result.outputRows);
            result.succeeded();
            repos.save(result);
            if (!event.doesNotDispatch()) {
                logQueue.send(new FakeS3Event(obj));
                result.dispatched();
                repos.save(result);
            }
            eventQueue.deleteAsync(event);
        }
        catch (S3IOException | IOException ex) {
            log.error("src: {}, error: {}", src.urlString(), ex.getMessage());
            result.failed(ex.getMessage());
            repos.save(result);
        }
    }

    S3ObjectMetadata applyFilter(ObjectFilter filter, S3ObjectLocation src, S3ObjectLocation dest, FilterResult result, String streamName) throws S3IOException, IOException {
        try (S3Agent.Buffer buf = s3.openWriteBuffer(dest, streamName)) {
            try (BufferedReader r = s3.openBufferedReader(src)) {
                filter.apply(r, buf.getBufferedWriter(), src.toString(), result);
            }
            return buf.commit();
        }
    }
}
