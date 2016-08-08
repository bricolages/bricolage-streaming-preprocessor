package org.bricolages.streaming.event;
import com.amazonaws.services.s3.event.*;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.LocalDateTime;
import lombok.*;
import lombok.extern.java.Log;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Slf4j
public class EventQueue {
    final SQSQueue queue;
    final Map<String, DeleteBufferEntry> deleteBuffer = new HashMap<String, DeleteBufferEntry>();
    static final int SQS_DELETE_BATCH_MAX = 10;
    static final int BUFFER_SIZE_MAX = SQS_DELETE_BATCH_MAX;
    static final int DELETE_MAX_RETRY_COUNT = 5;

    public List<Event> poll() {
        return stream().collect(Collectors.toList());
    }

    public Stream<Event> stream() {
        return convertMessages(queue.stream());
    }

    Stream<Event> convertMessages(Stream<Message> s) {
        return s.flatMap(Event::streamForMessage);
    }

    public void delete(Event event) {
        queue.deleteMessage(event.getReceiptHandle());
    }

    public void deleteAsync(Event event) {
        deleteBuffer.put(event.getMessageId(), new DeleteBufferEntry(event));
        if (isDeleteBufferFull()) {
            flushDelete();
        }
    }

    boolean isDeleteBufferFull() {
        return issueableEntryCount(LocalDateTime.now()) >= BUFFER_SIZE_MAX;
    }

    long issueableEntryCount(LocalDateTime now) {
        return deleteBuffer.values().stream().filter(ent -> ent.isIssueable(now)).count();
    }

    public void flushDelete() {
        if (deleteBuffer.isEmpty()) return;

        val now = LocalDateTime.now();
        val targetHandles = deleteBuffer.values().stream()
            .filter(ent -> ent.isIssueable(now))
            .map(ent -> new DeleteMessageBatchRequestEntry(ent.event.getMessageId(), ent.event.getReceiptHandle()))
            .collect(Collectors.toList());
        if (targetHandles.isEmpty()) return;

        // We must process ALL issuable buffered delete events here, because
        // the number of delete requests must be greater than the number of
        // receive requests if we take API failure into account.
        log.info("flushing async delete requests");
        while (! targetHandles.isEmpty()) {
            val handles = fetchItems(targetHandles, SQS_DELETE_BATCH_MAX);
            val result = queue.deleteMessageBatch(handles);
            for (val success : result.getSuccessful()) {
                deleteBuffer.remove(success.getId());
            }
            for (val failure : result.getFailed()) {
                val ent = deleteBuffer.get(failure.getId());
                if (ent == null) {
                    log.warn("MUST NOT HAPPEN: could not lookup DeleteBufferEntry: {}", failure.getId());
                    continue;
                }
                ent.failed();
                log.warn("SQS DeleteMessageBatch failed partially (count={}): {}", ent.failureCount, ent.event);
                if (ent.failureCount >= DELETE_MAX_RETRY_COUNT) {
                    log.warn("SQS DeleteMessageBatch failed too much; give up deleting message: {}", ent.event);
                    deleteBuffer.remove(failure.getId());
                }
            }
        }
        if (deleteBuffer.size() > BUFFER_SIZE_MAX * 10) {
            log.warn("SQS DeleteMessageBatch buffer size is too large: count={}", deleteBuffer.size());
        }
    }

    // Called on shutdown; issues and removes all pending delete requests, with ignoring all errors.
    public void flushDeleteForce() {
        log.info("*** Flushing async delete requests (forced)");

        val targetHandles = deleteBuffer.values().stream()
            .map(ent -> new DeleteMessageBatchRequestEntry(ent.event.getMessageId(), ent.event.getReceiptHandle()))
            .collect(Collectors.toList());

        int nFailure = 0;
        while (! targetHandles.isEmpty()) {
            val handles = fetchItems(targetHandles, SQS_DELETE_BATCH_MAX);
            val result = queue.deleteMessageBatch(handles);
            for (val success : result.getSuccessful()) {
                deleteBuffer.remove(success.getId());
            }
            for (val failure : result.getFailed()) {
                val ent = deleteBuffer.get(failure.getId());
                if (ent == null) {
                    log.warn("MUST NOT HAPPEN: could not lookup DeleteBufferEntry: {}", failure.getId());
                }
                else {
                    log.warn("SQS DeleteMessageBatch failed (message remains in the queue): {}", ent.event);
                }
                nFailure++;
                deleteBuffer.remove(failure.getId());
            }
        }
        if (! deleteBuffer.isEmpty()) {
            for (val ent : deleteBuffer.values()) {
                log.warn("MUST NOT HAPPEN: unhandled delete request: {}", ent.event);
            }
        }
        log.info("*** Async delete requests cleared (could not remove {} events)", nFailure);
    }

    <T> List<T> fetchItems(List<T> list, int count) {
        List<T> sublist = new ArrayList<T>();
        for (int i = 0; i < count; i++) {
            if (list.isEmpty()) break;
            sublist.add(list.remove(0));
        }
        return sublist;
    }

    static class DeleteBufferEntry {
        final Event event;
        int failureCount = 0;
        LocalDateTime lastRequested;
        LocalDateTime nextRequest;

        DeleteBufferEntry(Event event) {
            this.event = event;
        }

        void failed() {
            failureCount++;
            lastRequested = LocalDateTime.now();
            nextRequest = lastRequested.plusSeconds(nextIntervalSeconds());
        }

        boolean isIssueable(LocalDateTime now) {
            return (failureCount == 0) || nextRequest.isBefore(now);
        }

        long nextIntervalSeconds() {
            return (long)Math.pow(2, failureCount);
        }
    }
}
