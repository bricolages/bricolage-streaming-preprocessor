package org.bricolages.streaming;
import com.amazonaws.services.sqs.model.Message;

public class UnknownEvent extends Event {
    static public final class Parser implements MessageParser {
        @Override
        public boolean isCompatible(Message msg) {
            return true;
        }

        @Override
        public Event parse(Message msg) {
            return new UnknownEvent(msg);
        }
    }

    UnknownEvent(Message msg) {
        super(msg);
    }

    void callHandler(EventHandlers h) {
        h.handleUnknownEvent(this);
    }
}
