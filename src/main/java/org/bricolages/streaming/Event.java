package org.bricolages.streaming;
import com.amazonaws.services.sqs.model.Message;
import lombok.*;

@AllArgsConstructor
class Event {
    final Message message;

    String getMessageBody() {
        return message.getBody();
    }

    String getReceiptHandle() {
        return message.getReceiptHandle();
    }
}
