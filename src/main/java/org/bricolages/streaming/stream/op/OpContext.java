package org.bricolages.streaming.stream.op;

public interface OpContext {
    public String getStreamPrefix();
    public SequencialNumberRepository getSequencialNumberRepository();
}
