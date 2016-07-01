package org.bricolages.streaming.event;
import com.amazonaws.services.sqs.model.Message;

public class UnknownEvent extends Event {
    static final class Parser implements MessageParser {
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

    public void callHandler(EventHandlers h) {
        h.handleUnknownEvent(this);
    }
}
