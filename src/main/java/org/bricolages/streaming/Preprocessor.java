package org.bricolages.streaming;
import org.bricolages.streaming.filter.*;
import org.bricolages.streaming.event.*;
import org.bricolages.streaming.stream.*;
import org.bricolages.streaming.locator.*;
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
    final DataPacketRouter router;

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

    public boolean processUrl(S3ObjectLocator src, BufferedWriter out) {
        val route = router.routeWithoutDB(src);
        if (route == null) {
            log.warn("S3 object could not mapped: {}", src.toString());
            return false;
        }
        val filter = route.loadFilter();
        try {
            val result = filter.processLocatorAndPrint(src, out);
            log.debug("src: {}, dest: {}, in: {}, out: {}", src.toString(), route.getDestLocator().toString(), result.inputRows, result.outputRows);
            return true;
        }
        catch (IOException | LocatorIOException ex) {
            log.error("src: {}, error: {}", src.toString(), ex.getMessage());
            return false;
        }
    }

    boolean handleEvents() {
        boolean empty = true;
        for (val event : eventQueue.poll()) {
            try {
                log.debug("processing message: {}", event.getMessageBody());
                event.callHandler(this);
                empty = false;
            }
            catch (Exception ex) {
                log.error("unexpected exception: {}", ex.getMessage());
                ex.printStackTrace();
                safeSleep(3);   // to avoid busy loop by a bug
            }
        }
        return empty;
    }

    @Override
    public void handleUnknownEvent(UnknownEvent event) {
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
    FilterResultRepository logRepos;

    public void logNotMappedObject(String src) {
        log.warn("S3 object could not mapped: {}", src);
    }

    @Override
    public void handleS3Event(S3Event event) {
        log.info("handling URL: {}", event.getLocator());
        if (event.isCopyEvent()) {
            log.info("remove CopyEvent: {}", event.toString());
            eventQueue.deleteAsync(event);
            return;
        }

        S3ObjectLocator src = event.getLocator();
        BoundStream stream = router.route(src);
        if (stream == null) {
            // packet routing failed; this means invalid event or bad configuration.
            // We should remove invalid events from queue and
            // we must fix bad configuration by hand.
            // We cannot resolve latter case automatically, optimize for former case.
            //logNotMappedObject(src.toString());
            log.info("remove unmapped S3 object: {}", src.toString());
            eventQueue.deleteAsync(event);
            return;
        }
        if (stream.isBlackhole()) {
            // Should be removed by explicit configuration
            log.info("ignore event: {}", src.toString());
            eventQueue.deleteAsync(event);
            return;
        }
        val dest = stream.getDestLocator();

        if (stream.doesDefer()) {
            // Processing is temporary disabled; process objects later
            return;
        }
        if (stream.doesDiscard()) {
            // Just ignore without processing, do not keep SQS messages.
            log.info("discard event: {}", event.getLocator().toString());
            eventQueue.deleteAsync(event);
            return;
        }

        FilterResult result = new FilterResult(src.toString(), dest.toString());
        try {
            logRepos.save(result);

            S3ObjectMetadata meta = stream.processLocator(src, dest, result);
            log.debug("src: {}, dest: {}, in: {}, out: {}", src.toString(), dest.toString(), result.inputRows, result.outputRows);
            result.succeeded();
            logRepos.save(result);

            if (!event.doesNotDispatch() && !stream.doesNotDispatch()) {
                logQueue.send(new FakeS3Event(meta));
                result.dispatched();
                logRepos.save(result);
            }

            eventQueue.deleteAsync(event);
        }
        catch (LocatorIOException | IOException | ConfigError ex) {
            log.error("src: {}, error: {}", src.toString(), ex.getMessage());
            result.failed(ex.getMessage());
            logRepos.save(result);
        }
    }
}
