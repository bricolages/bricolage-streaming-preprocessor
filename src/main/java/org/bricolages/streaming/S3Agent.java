package org.bricolages.streaming;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import lombok.*;
import lombok.extern.java.Log;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class S3Agent {
    static final Charset DATA_FILE_CHARSET = StandardCharsets.UTF_8;
    static final String TMPDIR = "/tmp";   // FIXME: parameterize

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

    public BufferedReader openBufferedReader(S3ObjectLocation loc) throws IOException {
        InputStream in = openInputStream(loc);
        return new BufferedReader(new InputStreamReader(in, DATA_FILE_CHARSET));
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

    public void upload(Path src, S3ObjectLocation dest) throws IOException {
        s3client.putObject(dest.newPutRequest(src));
    }

    Buffer openWriteBuffer(S3ObjectLocation dest) throws IOException {
        Path tmp = Paths.get(TMPDIR, dest.basename());
        return new Buffer(tmp, dest);
    }

    @Getter
    final public class Buffer implements AutoCloseable {
        final Path path;
        final S3ObjectLocation dest;
        final BufferedWriter bufferedWriter;

        Buffer(Path path, S3ObjectLocation dest) throws IOException {
            this.path = path;
            this.dest = dest;

            Path tmp = Paths.get(TMPDIR, dest.basename());
            OutputStream s = Files.newOutputStream(tmp);
            OutputStream out = dest.isGzip() ? new GZIPOutputStream(s) : s;
            this.bufferedWriter = new BufferedWriter(new OutputStreamWriter(out, DATA_FILE_CHARSET));
        }

        public void commit() throws IOException {
            bufferedWriter.close();   // flush() does not work
            upload(path, dest);
        }

        @Override
        public void close() {
            try {
                bufferedWriter.close();
            }
            catch (IOException ex) {
                // ignore
            }
            try {
                Files.deleteIfExists(path);
            }
            catch (IOException ex) {
                log.error("could not remove temporary file: {}", ex);
            }
        }
    }
}
