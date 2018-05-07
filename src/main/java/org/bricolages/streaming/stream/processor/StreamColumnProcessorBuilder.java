package org.bricolages.streaming.stream.processor;
import java.util.function.BiFunction;

public interface StreamColumnProcessorBuilder extends BiFunction<ProcessorParams, ProcessorContext, StreamColumnProcessor> {
}
