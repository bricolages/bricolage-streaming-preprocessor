package org.bricolages.streaming;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.nio.file.Paths;
import java.util.zip.GZIPInputStream;

import lombok.*;

public class LocalFileSourceLocator implements SourceLocator {
    private final URI fileUrl;

    LocalFileSourceLocator(String filepath) throws IOException {
        // /path/to/file -> file:/path/to/file
        // ./relative/file -> file:/path/to/relative/file
        this.fileUrl = new File(filepath).getCanonicalFile().toURI();
    }

    public BufferedReader open() throws IOException {
        val inputStream = new FileInputStream(fileUrl.getPath());
        if (isGzip()) {
            return new BufferedReader(new InputStreamReader(new GZIPInputStream(inputStream)));
        } else {
            return new BufferedReader(new InputStreamReader(inputStream));
        }
    }

    private boolean isGzip() {
        return fileUrl.toString().endsWith(".gz");
    }

    public String toString() {
        return this.fileUrl.toString();
    }
}
