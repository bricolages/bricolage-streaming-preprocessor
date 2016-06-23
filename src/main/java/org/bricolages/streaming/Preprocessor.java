package org.bricolages.streaming;

import com.amazonaws.auth.profile.*;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.nio.file.*;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import lombok.*;

public class Preprocessor {
    static public void main(String[] args) throws Exception {
        S3ObjectLocation loc = new S3ObjectLocation(args[0], args[1]);
        Preprocessor pp = new Preprocessor();
        //pp.download(loc);
        //pp.s3cat(loc);
        S3ObjectLocation d = new S3ObjectLocation(args[2], args[3]);
        pp.s3filter(loc, d);
    }

    final AmazonS3 s3client;
    final ObjectMapper mapper;

    public Preprocessor() {
        super();
        this.s3client = new AmazonS3Client(new ProfileCredentialsProvider());
        this.mapper = new ObjectMapper();
    }

    void download(S3ObjectLocation loc) throws IOException {
        try (InputStream in = s3open(loc)) {
            Files.copy(in, loc.basename());
        }
    }

    void s3cat(S3ObjectLocation loc) throws IOException {
        OutputStream out = System.out;
        try (InputStream in = s3open(loc)) {
            GZIPInputStream gz = new GZIPInputStream(in);
            byte[] buf = new byte[4096];
            int n;
            while ((n = gz.read(buf)) >= 0) {
                out.write(buf, 0, n);
            }
        }
    }

    void s3filter(S3ObjectLocation src, S3ObjectLocation dest) throws IOException {
        try (BufferedWriter out = Files.newBufferedWriter(dest.basename(), Charset.defaultCharset())) {
            PrintWriter w = new PrintWriter(out);
            try (InputStream in = s3open(src)) {
                BufferedReader r = new BufferedReader(new InputStreamReader(new GZIPInputStream(in)));
                filterStream(r, w);
            }
        }
    }

    void filterStream(BufferedReader r, PrintWriter w) throws IOException {
        r.lines().forEach((line) -> {
            String result = filterJsonString(line);
            if (result != null) {
                w.println(result);
            }
        });
    }

    String filterJsonString(String json) {
        try {
            Map<String, Object> obj = mapper.readValue(json, Map.class);
            Object result = filterJsonObject(obj);
            return mapper.writeValueAsString(result);
        }
        catch (JsonProcessingException ex) {
            // FIXME
            return null;
        }
        catch (IOException ex) {
            // FIXME
            return null;
        }
    }

    Object filterJsonObject(Map<String, Object> obj) {
        obj.put("extra", "value");
        return obj;
    }

    InputStream s3open(S3ObjectLocation loc) throws IOException {
        S3Object obj = s3client.getObject(loc.getRequest());
        return obj.getObjectContent();
    }

    @AllArgsConstructor
    @Getter
    static class S3ObjectLocation {
        final String bucket;
        final String key;

        Path basename() {
            return Paths.get(key).getFileName();
        }

        GetObjectRequest getRequest() {
            return new GetObjectRequest(bucket, key);
        }
    }
}
