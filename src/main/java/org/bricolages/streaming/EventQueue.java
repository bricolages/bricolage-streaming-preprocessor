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
    public Stream<Event> stream() {
        return convertMessages(queue.stream());
    }

    public Stream<Event> finiteStream() throws IOException {
        return convertMessages(queue.receiveMessages().stream());
    }

    Stream<Event> convertMessages(Stream<Message> s) {
        return s.flatMap(Event::streamForMessage);
    }

    void commit(Event event) {
        queue.deleteMessage(event.getReceiptHandle());
    }
}
