package org.bricolages.streaming;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.charset.Charset;
import java.util.zip.GZIPInputStream;
import lombok.*;
import lombok.extern.java.Log;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class S3Agent {
    final AmazonS3 s3client;

    public S3Agent(AWSCredentialsProvider credentials) {
        super();
        this.s3client = new AmazonS3Client(credentials);
    }

    public void download(S3ObjectLocation src, Path dest) throws IOException {
        try (InputStream in = openInputStream(src)) {
            Files.copy(in, dest);
        }
    }

    public void upload(Path src, S3ObjectLocation dest) throws IOException {
        s3client.putObject(dest.newPutRequest(src));
    }

    public BufferedReader openBufferedReader(S3ObjectLocation loc, Charset cs) throws IOException {
        InputStream in = openInputStream(loc);
        return new BufferedReader(new InputStreamReader(in, cs));
    }

    public InputStream openInputStream(S3ObjectLocation loc) throws IOException {
        InputStream in = openInputStreamRaw(loc);
        if (loc.isGzip()) {
            log.debug("reading gzip: {}", loc);
            return new GZIPInputStream(in);
        }
        else {
            log.debug("reading raw: {}", loc);
            return in;
        }
    }

    public InputStream openInputStreamRaw(S3ObjectLocation loc) throws IOException {
        S3Object obj = s3client.getObject(loc.newGetRequest());
        return obj.getObjectContent();
    }
}
