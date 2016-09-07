package org.bricolages.streaming.event;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Slf4j
public class LogQueue {
    final SQSQueue queue;

    public void send(LogQueueEvent e) {
        queue.sendMessage(e.messageBody());
    }
}
