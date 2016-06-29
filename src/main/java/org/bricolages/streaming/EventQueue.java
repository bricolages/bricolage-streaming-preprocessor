package org.bricolages.streaming;
import com.amazonaws.services.s3.event.*;
import com.amazonaws.services.sqs.model.Message;
import java.util.stream.Stream;
import java.io.IOException;
import java.io.UncheckedIOException;
import lombok.*;
import lombok.extern.java.Log;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Slf4j
class EventQueue {
    final SQSQueue queue;

    // FIXME: <Event>
    public Stream<S3Event> stream() {
        return convertMessages(queue.stream());
    }

    // FIXME: <Event>
    public Stream<S3Event> finiteStream() throws IOException {
        return convertMessages(queue.receiveMessages().stream());
    }

    // FIXME: <Event>
    Stream<S3Event> convertMessages(Stream<Message> s) {
        return s.flatMap(msg -> {
            // FIXME: polymorphic
            try {
                if (msg.getBody().contains("ObjectCreated:")) {
                    return Stream.of(S3Event.forMessage(msg));
                }
                else {
                    log.warn("unknown kind of message: {}", msg.getBody());
                    return Stream.empty();
                }
            }
            catch (IOException ex) {
                log.error("could not map SQS message", ex);
                return Stream.empty();
            }
        });
    }

    void commit(Event event) {
        queue.deleteMessage(event.getReceiptHandle());
    }
}
