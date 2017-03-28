package org.bricolages.streaming;

import java.io.BufferedReader;
import java.io.IOException;

import org.bricolages.streaming.s3.S3Agent;
import org.bricolages.streaming.s3.S3IOException;
import org.bricolages.streaming.s3.S3ObjectLocation;

public class S3ObjectSourceLocator implements SourceLocator {
    private final S3Agent agent;
    private final S3ObjectLocation location;

    S3ObjectSourceLocator(S3Agent agent, S3ObjectLocation location) {
        this.agent = agent;
        this.location = location;
    }

    public BufferedReader open() throws IOException {
        try {
            return agent.openBufferedReader(location);
        } catch (S3IOException ex) {
            throw new IOException(ex);
        }
    }

    public String toString() {
        return location.toString();
    }
}
