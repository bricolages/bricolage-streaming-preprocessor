package org.bricolages.streaming.s3;
import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
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
import java.nio.charset.CodingErrorAction;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class S3Agent {
    static final Charset DATA_FILE_CHARSET = StandardCharsets.UTF_8;
    static final String TMPDIR = "/tmp";   // FIXME: parameterize

    final AmazonS3 s3client;

    public void download(S3ObjectLocation src, Path dest) throws S3IOException {
        try {
            try (InputStream in = openInputStream(src)) {
                Files.copy(in, dest);
            }
        }
        catch (IOException ex) {
            throw new S3IOException("I/O error: " + ex.getMessage());
        }
    }

    public BufferedReader openBufferedReader(S3ObjectLocation loc) throws S3IOException {
        InputStream in = openInputStreamAuto(loc);
        // CharsetDecoder is not multi-threads safe, create new instance always.
        val decoder = DATA_FILE_CHARSET.newDecoder();
        decoder.onMalformedInput(CodingErrorAction.REPLACE);
        decoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
        return new BufferedReader(new InputStreamReader(in, decoder));
    }

    InputStream openInputStreamAuto(S3ObjectLocation loc) throws S3IOException {
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
        catch (IOException ex) {
            throw new S3IOException("I/O error: " + ex.getMessage());
        }
    }

    public InputStream openInputStream(S3ObjectLocation loc) throws S3IOException {
        try {
            S3Object obj = s3client.getObject(loc.newGetRequest());
            return obj.getObjectContent();
        }
        catch (AmazonClientException ex) {
            throw new S3IOException("S3 error in GetObject: " + ex.getMessage());
        }
    }

    public void upload(Path src, S3ObjectLocation dest) throws S3IOException {
        try {
            s3client.putObject(dest.newPutRequest(src));
        }
        catch (AmazonClientException ex) {
            throw new S3IOException("S3 error in PutObject: " + ex.getMessage());
        }
    }

    public Buffer openWriteBuffer(S3ObjectLocation dest) throws S3IOException {
        Path tmp = Paths.get(TMPDIR, dest.basename());
        return new Buffer(tmp, dest);
    }

    @Getter
    final public class Buffer implements AutoCloseable {
        final Path path;
        final S3ObjectLocation dest;
        final BufferedWriter bufferedWriter;

        Buffer(Path path, S3ObjectLocation dest) throws S3IOException {
            this.path = path;
            this.dest = dest;

            try {
                Path tmp = Paths.get(TMPDIR, dest.basename());
                OutputStream s = Files.newOutputStream(tmp);
                OutputStream out = dest.isGzip() ? new GZIPOutputStream(s) : s;
                this.bufferedWriter = new BufferedWriter(new OutputStreamWriter(out, DATA_FILE_CHARSET));
            }
            catch (IOException ex) {
                throw new S3IOException("I/O error: " + ex.getMessage());
            }
        }

        public void commit() throws S3IOException {
            try {
                bufferedWriter.close();   // flush() does not work
                upload(path, dest);
            }
            catch (IOException ex) {
                throw new S3IOException("I/O error: " + ex.getMessage());
            }
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
