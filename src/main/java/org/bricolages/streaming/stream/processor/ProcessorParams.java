package org.bricolages.streaming.stream.processor;

public interface ProcessorParams {
    String getName();
    String getSourceName();
    String getType();
    Integer getLength();
    String getSourceOffset();
    String getZoneOffset();
    String getTimeUnit();
}
