package org.bricolages.streaming;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;

import lombok.*;

public class LocalFileSourceLocator implements SourceLocator {
    private final String fileUrl;

    LocalFileSourceLocator(String filepath) throws IOException {
        // /path/to/file -> file:/path/to/file
        // ./relative/file -> file:/path/to/relative/file
        this.fileUrl = new File(filepath).getCanonicalFile().toURI().toString();
    }

    public BufferedReader open() throws IOException {
        val filereader = new FileReader(fileUrl);
		return new BufferedReader(filereader);
    }

    public String toString() {
        return this.fileUrl;
    }
}
