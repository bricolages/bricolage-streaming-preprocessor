package org.bricolages.streaming;
import com.amazonaws.services.sqs.model.Message;

public class ShutdownEvent extends Event {
    static public final class Parser implements MessageParser {
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

    void callHandler(EventHandlers h) {
        h.handleShutdownEvent(this);
    }
}
