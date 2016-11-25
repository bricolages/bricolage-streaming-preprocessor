package org.bricolages.streaming;

import lombok.Data;
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

    private final EventQueue eventQueue = new EventQueue();
    private final LogQueue logQueue = new LogQueue();
    private List<Mapping> mappings = new ArrayList<>();

    // Must be public for Spring
    public EventQueue getEventQueue() {
        return eventQueue;
    }

    public LogQueue getLogQueue() {
        return logQueue;
    }

    public List<Mapping> getMappings() {
        return mappings;
    }

    public void setMappings(List<Mapping> mappings) {
        this.mappings = mappings;
    }

    //TODO Remove *Entry() methods
    EventQueueEntry getEventQueueEntry() {
        return new EventQueueEntry(getEventQueue());
    }

    LogQueueEntry getLogQueueEntry() {
        return new LogQueueEntry(getLogQueue());
    }

    List<ObjectMapper.Entry> getMappingEntries() {
        List<ObjectMapper.Entry> entries = new ArrayList<>();
        for (Mapping m : getMappings()) {
            ObjectMapper.Entry e = new ObjectMapper.Entry();
            e.setSrc(m.src);
            e.setDest(m.dest);
            e.setTable(m.table);
            entries.add(e);
        }
        return entries;
    }

    @Getter
    @Setter
    static class EventQueue {
        private String url;
        private int visibilityTimeout;
        private int maxNumberOfMessages;
        private int waitTimeSeconds;
    }

    class EventQueueEntry {
        public final String url;
        public final int visibilityTimeout;
        public final int maxNumberOfMessages;
        public final int waitTimeSeconds;

        public EventQueueEntry(EventQueue eq) {
            this.url = eq.getUrl();
            this.visibilityTimeout = eq.getVisibilityTimeout();
            this.maxNumberOfMessages = eq.getMaxNumberOfMessages();
            this.waitTimeSeconds = eq.getWaitTimeSeconds();
        }
    }

    @Getter
    @Setter
    static class LogQueue {
        private String url;
    }

    class LogQueueEntry {
        public final String url;

        public LogQueueEntry(LogQueue sq) {
            this.url = sq.getUrl();
        }
    }

    @Getter
    @Setter
    // Must be pubic (Don't know why...)
    public static class Mapping {
        private String src;
        private String dest;
        private String table;
    }
}
