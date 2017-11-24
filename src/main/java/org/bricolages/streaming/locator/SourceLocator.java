package org.bricolages.streaming.locator;
import java.io.BufferedReader;
import java.io.IOException;

public interface SourceLocator {
    BufferedReader open() throws IOException;

    String toString();
}
