package org.bricolages.streaming.event;
import com.amazonaws.services.sqs.model.Message;

interface MessageParser {
    boolean isCompatible(Message msg);
    Event parse(Message msg) throws MessageParseException;
}
