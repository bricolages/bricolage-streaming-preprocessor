package org.bricolages.streaming;
import com.amazonaws.services.sqs.model.Message;
import java.util.stream.Stream;
import java.util.List;
import java.util.ArrayList;
import lombok.*;
import lombok.extern.java.Log;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Slf4j
abstract class Event {
    static final List<MessageParser> PARSERS = new ArrayList<MessageParser>();
    static {
        PARSERS.add(new S3Event.Parser());
        PARSERS.add(new ShutdownEvent.Parser());
        PARSERS.add(new UnknownEvent.Parser());
    }

    static public Stream<Event> streamForMessage(Message msg) {
        try {
            for (MessageParser parser : PARSERS) {
                if (parser.isCompatible(msg)) {
                    Event e = parser.parse(msg);
                    return (e == null) ? Stream.empty() : Stream.of(e);
                }
            }
            return Stream.empty();
        }
        catch (MessageParseException ex) {
            // FIXME: notify?
            log.error("could not interpret SQS message", ex);
            return Stream.empty();
        }
    }

    final Message message;

    public String getMessageBody() {
        return message.getBody();
    }

    public String getReceiptHandle() {
        return message.getReceiptHandle();
    }

    abstract void callHandler(EventHandlers h);
}
