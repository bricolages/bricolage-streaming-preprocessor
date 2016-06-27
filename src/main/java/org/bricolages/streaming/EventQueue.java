package org.bricolages.streaming;
import com.amazonaws.services.s3.event.*;
import java.util.stream.Stream;
import java.io.IOException;
import java.io.UncheckedIOException;
import lombok.*;

@AllArgsConstructor
class EventQueue {
    final SQSQueue queue;

    // FIXME: <Event>
    Stream<S3Event> events() {
        return queue.stream().map(msg -> {
            // FIXME: polymorphic
            try {
                return S3Event.forMessage(msg);
            }
            catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
        });
    }

    void commit(Event event) {
        queue.deleteMessage(event.getReceiptHandle());
    }
}
