package org.bricolages.streaming.preproc;
import org.bricolages.streaming.event.*;
import org.bricolages.streaming.stream.*;
import org.bricolages.streaming.object.*;
import org.bricolages.streaming.exception.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.dao.DataIntegrityViolationException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.util.Objects;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class Preprocessor implements EventHandlers {
    final EventQueue eventQueue;
    final LogQueue logQueue;
    final PacketRouter router;

    public void run() {
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
            log.debug("src: {}, dest: {}, in: {}, out: {}, err: {}", src.toString(), route.getDestLocator().toString(), result.getInputRows(), result.getOutputRows(), result.getErrorRows());
            return true;
        }
        catch (ObjectIOException ex) {
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
    PreprocMessageRepository msgRepos;

    @Autowired
    PreprocJobRepository jobRepos;

    @Autowired
    PacketRepository packetRepos;

    @Autowired
    ChunkRepository chunkRepos;

    @Autowired
    StreamColumnRepository columnRepos;

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

        PreprocMessage msg = acceptMessage(event);

        S3ObjectLocator src = event.getLocator();
        BoundStream stream = router.route(src);
        if (stream == null) {
            // packet routing failed; this means invalid event or bad configuration.
            // We should remove invalid events from queue and
            // we must fix bad configuration by hand.
            // We cannot resolve latter case automatically, optimize for former case.
            //logNotMappedObject(src.toString());
            log.info("remove unmapped S3 object: {}", src.toString());
            deleteMessage(msg, event);
            return;
        }
        if (stream.isBlackhole()) {
            // Should be removed by explicit configuration
            log.info("ignore event: {}", src.toString());
            deleteMessage(msg, event);
            return;
        }

        streamDetected(msg, stream);
        val dest = stream.getDestLocator();
        if (stream.isNotInitialized()) {
            log.info("discard event for uninitialized stream: {}", event.getLocator().toString());
            deleteMessage(msg, event);
            return;
        }
        if (stream.isDisabled()) {
            // Processing is temporary disabled; process objects later
            return;
        }
        if (stream.doesDiscard()) {
            // Just ignore without processing, do not keep SQS messages.
            log.info("discard event: {}", event.getLocator().toString());
            deleteMessage(msg, event);
            return;
        }

        PreprocJob job = jobStarted(msg, event, stream);
        try {
            PacketFilterResult result = stream.processLocator(src, dest);
            log.debug("src: {}, dest: {}, in: {}, out: {}, err: {}", src.toString(), dest.toString(), result.getInputRows(), result.getOutputRows(), result.getErrorRows());
            Chunk chunk = jobSucceeded(job, stream, result);

            if (!event.doesNotDispatch() && !stream.doesNotDispatch()) {
                dispatch(result, chunk);
            }

            deleteMessage(msg, event);

            columnRepos.saveUnknownColumns(stream.getStream(), result.getUnknownColumns());
        }
        catch (ObjectIOException | ConfigError ex) {
            log.error("src: {}, error: {}", src.toString(), ex.getMessage());
            jobFailed(job, ex.getMessage());
        }
    }

    PreprocMessage acceptMessage(S3Event event) {
        return msgRepos.upsert(new PreprocMessage(event.getMessageId(), event.getObjectMetadata()));
    }

    void streamDetected(PreprocMessage msg, BoundStream stream) {
        msg.changeStateToStreamDetected(stream.getStream());
        msgRepos.save(msg);
    }

    void deleteMessage(PreprocMessage msg, S3Event event) {
        eventQueue.deleteAsync(event);

        msg.changeStateToHandled();
        msgRepos.save(msg);
    }

    PreprocJob jobStarted(PreprocMessage msg, S3Event event, BoundStream stream) {
        Packet packet = packetRepos.upsert(new Packet(event.getObjectMetadata(), stream));
        msg.changeStateToJobStarted(packet);   // defer to save
        return jobRepos.save(new PreprocJob(msg, packet));
    }

    Chunk jobSucceeded(PreprocJob job, BoundStream stream, PacketFilterResult result) {
        Chunk chunk = chunkRepos.upsert(new Chunk(stream.getTableId(), result));

        Packet packet = job.getPacket();
        packet.changeStateToProcessed(chunk);
        packetRepos.save(packet);

        job.changeStateToSucceeded();
        jobRepos.save(job);

        return chunk;
    }

    void jobFailed(PreprocJob job, String message) {
        job.changeStateToFailed(message);
        jobRepos.save(job);
    }

    void dispatch(PacketFilterResult result, Chunk chunk) {
        logQueue.send(new FakeS3Event(result.getObjectMetadata()));

        chunk.changeStateToDispatched();
        chunkRepos.save(chunk);
    }
}
