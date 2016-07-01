package org.bricolages.streaming.event;
import com.amazonaws.services.sqs.model.Message;

public class ShutdownEvent extends Event {
    static final class Parser implements MessageParser {
        @Override
        public boolean isCompatible(Message msg) {
            return msg.getBody().contains("\"eventName\":\"shutdown");
        }

        @Override
        public Event parse(Message msg) {
            return new ShutdownEvent(msg);
        }
    }

    ShutdownEvent(Message msg) {
        super(msg);
    }

    public void callHandler(EventHandlers h) {
        h.handleShutdownEvent(this);
    }
}
