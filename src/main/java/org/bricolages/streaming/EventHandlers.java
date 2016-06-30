package org.bricolages.streaming;

interface EventHandlers {
    void handleS3Event(S3Event e);
    void handleShutdownEvent(ShutdownEvent e);
    void handleUnknownEvent(UnknownEvent e);
}
