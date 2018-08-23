package org.bricolages.streaming.object;
import java.net.URI;
import java.io.File;
import java.io.IOException;
import lombok.*;

public class LocalFileLocator extends Locator {
    final URI fileUrl;

    LocalFileLocator(String filepath) throws IOException {
        // /path/to/file -> file:/path/to/file
        // ./relative/file -> file:/path/to/relative/file
        this.fileUrl = new File(filepath).getCanonicalFile().toURI();
    }

    @Override public String toString() { return this.fileUrl.toString(); }
    @Override public boolean isLocalFile() { return true; }
    @Override public boolean isS3Object() { return false; }
    @Override public S3ObjectLocator asS3ObjectLocator() { return null; }
}
