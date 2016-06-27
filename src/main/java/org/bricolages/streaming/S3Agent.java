package org.bricolages.streaming;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.event.*;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.zip.GZIPInputStream;
import lombok.*;

class S3Agent {
    final AmazonS3 s3client;

    public S3Agent(AWSCredentialsProvider credentials) {
        super();
        this.s3client = new AmazonS3Client(credentials);
    }

    void download(S3ObjectLocation src, Path dest) throws IOException {
        try (InputStream in = openInputStream(src)) {
            Files.copy(in, dest);
        }
    }

    BufferedReader openBufferedReader(S3ObjectLocation loc) throws IOException {
        InputStream in = openInputStream(loc);
        return new BufferedReader(new InputStreamReader(in));
    }

    InputStream openInputStream(S3ObjectLocation loc) throws IOException {
        InputStream in = openInputStreamRaw(loc);
        return loc.isGzip() ? new GZIPInputStream(in) : in;
    }

    InputStream openInputStreamRaw(S3ObjectLocation loc) throws IOException {
        S3Object obj = s3client.getObject(loc.getRequest());
        return obj.getObjectContent();
    }
}
