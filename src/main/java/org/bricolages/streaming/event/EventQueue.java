package org.bricolages.streaming.event;
import com.amazonaws.services.s3.event.*;
import com.amazonaws.services.sqs.model.Message;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.io.IOException;
import java.io.UncheckedIOException;
import lombok.*;
import lombok.extern.java.Log;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Slf4j
public class EventQueue {
    final SQSQueue queue;

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
}
