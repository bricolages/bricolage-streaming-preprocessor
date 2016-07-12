package org.bricolages.streaming.event;
import com.amazonaws.services.s3.event.*;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;
import java.util.List;
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
    static final int BUFFER_SIZE_MAX = 10;   // SQS system limit
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
        val handles = deleteBuffer.values().stream()
            .filter(ent -> ent.isIssueable(now))
            .map(ent -> new DeleteMessageBatchRequestEntry(ent.event.getMessageId(), ent.event.getReceiptHandle()))
            .limit(BUFFER_SIZE_MAX)
            .collect(Collectors.toList());
        if (handles.isEmpty()) return;
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
        if (deleteBuffer.size() > BUFFER_SIZE_MAX * 10) {
            log.warn("SQS DeleteMessageBatch buffer size is too large: count={}", deleteBuffer.size());
        }
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
            return (failureCount == 0) || lastRequested.isBefore(now);
        }

        long nextIntervalSeconds() {
            return (long)Math.pow(2, failureCount);
        }
    }
}
