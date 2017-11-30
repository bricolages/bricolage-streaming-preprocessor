package org.bricolages.streaming;
import org.bricolages.streaming.filter.*;
import org.bricolages.streaming.event.*;
import org.bricolages.streaming.stream.*;
import org.bricolages.streaming.locator.*;
import org.bricolages.streaming.s3.*;
import org.bricolages.streaming.exception.*;
import org.springframework.beans.factory.annotation.Autowired;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Objects;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class Preprocessor implements EventHandlers {
    final EventQueue eventQueue;
    final LogQueue logQueue;
    final S3Agent s3;
    final DataPacketRouter router;
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
        val srcUrl = src.urlString();
        val route = router.routeWithoutDB(src);
        if (route == null) {
            log.warn("S3 object could not mapped: {}", srcUrl);
            return false;
        }

        FilterResult result = new FilterResult(srcUrl, null);
        try {
            ObjectFilter filter = filterFactory.load(route.getStream());
            try (BufferedReader r = s3.openBufferedReader(src)) {
                filter.apply(r, out, srcUrl, result);
            }
            log.debug("src: {}, dest: {}, in: {}, out: {}", srcUrl, route.getDestLocation().urlString(), result.inputRows, result.outputRows);
            return true;
        }
        catch (IOException | S3IOException ex) {
            log.error("src: {}, error: {}", srcUrl, ex.getMessage());
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

    public void logNotMappedObject(String src) {
        log.warn("S3 object could not mapped: {}", src);
    }

    @Override
    public void handleS3Event(S3Event event) {
        log.debug("handling URL: {}", event.getLocation().toString());
        if (event.isCopyEvent()) {
            log.info("remove CopyEvent: {}", event.toString());
            eventQueue.deleteAsync(event);
            return;
        }

        S3ObjectLocation src = event.getLocation();
        val route = router.route(src);
        if (route == null) {
            // packet routing failed; this means invalid event or bad configuration.
            // We should remove invalid events from queue and
            // we must fix bad configuration by hand.
            // We cannot resolve latter case automatically, optimize for former case.
            //logNotMappedObject(src.toString());
            log.info("remove unmapped S3 object: {}", src.toString());
            eventQueue.deleteAsync(event);
            return;
        }
        val stream = route.getStream();
        val dest = route.getDestLocation();

        if (stream.doesDiscard()) {
            // Just ignore without processing, do not keep SQS messages.
            log.info("discard event: {}", event.getLocation().toString());
            eventQueue.deleteAsync(event);
            return;
        }

        FilterResult result = new FilterResult(src.urlString(), dest.urlString());
        try {
            repos.save(result);
            ObjectFilter filter = filterFactory.load(stream);
            S3ObjectMetadata obj = applyFilter(filter, src, dest, result, stream.getStreamName());
            log.debug("src: {}, dest: {}, in: {}, out: {}", src.urlString(), dest.urlString(), result.inputRows, result.outputRows);
            result.succeeded();
            repos.save(result);
            if (!event.doesNotDispatch() && !stream.doesNotDispatch()) {
                logQueue.send(new FakeS3Event(obj));
                result.dispatched();
                repos.save(result);
            }
            eventQueue.deleteAsync(event);
        }
        catch (S3IOException | IOException | ConfigError ex) {
            log.error("src: {}, error: {}", src.urlString(), ex.getMessage());
            result.failed(ex.getMessage());
            repos.save(result);
        }
    }

    public S3ObjectMetadata applyFilter(ObjectFilter filter, S3ObjectLocation src, S3ObjectLocation dest, FilterResult result, String streamName) throws S3IOException, IOException {
        try (S3Agent.Buffer buf = s3.openWriteBuffer(dest, streamName)) {
            try (BufferedReader r = s3.openBufferedReader(src)) {
                filter.apply(r, buf.getBufferedWriter(), src.urlString(), result);
            }
            return buf.commit();
        }
    }
}
