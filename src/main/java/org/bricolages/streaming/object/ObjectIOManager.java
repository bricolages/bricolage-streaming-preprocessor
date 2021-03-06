package org.bricolages.streaming.object;
import org.bricolages.streaming.exception.ApplicationAbort;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.charset.CodingErrorAction;
import java.time.Instant;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class ObjectIOManager {
    static final Charset DATA_FILE_CHARSET = StandardCharsets.UTF_8;
    static final String TMPDIR = "/tmp";   // FIXME: parameterize

    final AmazonS3 s3client;

    public void download(S3ObjectLocator src, Path dest) throws ObjectIOException {
        try {
            try (InputStream in = openInputStream(src)) {
                Files.copy(in, dest);
            }
        }
        catch (UncheckedIOException | IOException ex) {
            throw new ObjectIOException("I/O error: " + ex.getMessage());
        }
    }

    public BufferedReader openBufferedReader(S3ObjectLocator loc) throws ObjectIOException {
        InputStream in = openInputStreamAuto(loc);
        // CharsetDecoder is not multi-threads safe, create new instance always.
        val decoder = DATA_FILE_CHARSET.newDecoder();
        decoder.onMalformedInput(CodingErrorAction.REPLACE);
        decoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
        return new BufferedReader(new InputStreamReader(in, decoder));
    }

    InputStream openInputStreamAuto(S3ObjectLocator loc) throws ObjectIOException {
        try {
            InputStream in = openInputStream(loc);
            if (loc.isGzip()) {
                log.debug("reading gzip: {}", loc);
                return new GZIPInputStream(in);
            }
            else {
                log.debug("reading raw: {}", loc);
                return in;
            }
        }
        catch (UncheckedIOException | IOException ex) {
            throw new ObjectIOException("I/O error: " + ex.getMessage());
        }
    }

    public InputStream openInputStream(S3ObjectLocator loc) throws ObjectIOException {
        try {
            val req = new GetObjectRequest(loc.bucket(), loc.key());
            val obj = getObjectWithRetry(req);
            return obj.getObjectContent();
        }
        catch (AmazonClientException ex) {
            log.error(ex.getMessage());
            throw new ObjectIOException("S3 error in GetObject: " + ex.getMessage());
        }
    }

    static final int RETRY_MAX = 2;

    S3Object getObjectWithRetry(GetObjectRequest req) throws AmazonClientException {
        int nRetry = 0;
        while (true) {
            try {
                return s3client.getObject(req);
            }
            catch (AmazonServiceException ex) {
                // S3 is an eventual consistency system, GetObject may fail just after ObjectCreated event occured.
                if (ex.getErrorCode().equals("NoSuchKey") && nRetry < RETRY_MAX) {
                    log.info("GetObject failed, retrying...");
                    safeSleep(3);
                    nRetry++;
                    continue;
                }
                throw ex;
            }
        }
    }

    void safeSleep(int sec) {
        try {
            log.info("sleeping " + sec + " seconds");
            Thread.sleep(sec * 1000);
        }
        catch (InterruptedException ex) {
            throw new ApplicationAbort("sleep interrupted");
        }
    }

    public PutObjectResult upload(Path src, S3ObjectLocator dest) throws ObjectIOException {
        try {
            val req = new PutObjectRequest(dest.bucket(), dest.key(), src.toFile());
            return s3client.putObject(req);
        }
        catch (AmazonClientException ex) {
            throw new ObjectIOException("S3 error in PutObject: " + ex.getMessage());
        }
    }

    public Buffer openWriteBuffer(S3ObjectLocator dest) throws ObjectIOException {
        String base = dest.key().replaceAll("/", "-");
        // I believe object key does not include multi-byte characters!
        if (base.length() > 255) {
            base = base.substring(base.length() - 255);
        }
        Path tmp = Paths.get(TMPDIR, base);
        return new Buffer(tmp, dest);
    }

    @Getter
    final public class Buffer implements AutoCloseable {
        final Path path;
        final S3ObjectLocator dest;
        final BufferedWriter bufferedWriter;

        Buffer(Path path, S3ObjectLocator dest) throws ObjectIOException {
            this.path = path;
            this.dest = dest;

            try {
                OutputStream s = Files.newOutputStream(path);
                OutputStream out = dest.isGzip() ? new GZIPOutputStream(s) : s;
                this.bufferedWriter = new BufferedWriter(new OutputStreamWriter(out, DATA_FILE_CHARSET));
            }
            catch (UncheckedIOException | IOException ex) {
                throw new ObjectIOException("I/O error: " + ex.getMessage());
            }
        }

        public S3ObjectMetadata commit() throws ObjectIOException {
            try {
                bufferedWriter.close();   // flush() does not work
                val result = upload(path, dest);
                return new S3ObjectMetadata(dest, Instant.now(), Files.size(path), result.getETag());
            }
            catch (UncheckedIOException | IOException ex) {
                throw new ObjectIOException("I/O error: " + ex.getMessage());
            }
        }

        @Override
        public void close() {
            try {
                bufferedWriter.close();
            }
            catch (UncheckedIOException | IOException ex) {
                // ignore
            }
            try {
                Files.deleteIfExists(path);
            }
            catch (UncheckedIOException | IOException ex) {
                log.error("could not remove temporary file: {}", ex);
            }
        }
    }
}
