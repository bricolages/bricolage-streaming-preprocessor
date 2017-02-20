package org.bricolages.streaming;

import lombok.Getter;
import lombok.Setter;

import org.bricolages.streaming.s3.ObjectMapper;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@ConfigurationProperties(prefix = "bricolage")
public class Config {
    @Getter
    private final EventQueue eventQueue = new EventQueue();
    @Getter
    private final LogQueue logQueue = new LogQueue();
    @Getter
    private List<ObjectMapper.Entry> mappings = new ArrayList<>();

    @Setter
    static class EventQueue {
        String url;
        int visibilityTimeout;
        int maxNumberOfMessages;
        int waitTimeSeconds;
    }

    @Setter
    static class LogQueue {
        String url;
    }
}
