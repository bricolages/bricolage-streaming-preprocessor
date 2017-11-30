package org.bricolages.streaming.locator;
import java.io.BufferedReader;
import java.io.IOException;

public interface SourceLocator {
    BufferedReader open() throws IOException;

    String toString();

    default S3ObjectSourceLocator asS3Object() {
        return null;
    }
}
