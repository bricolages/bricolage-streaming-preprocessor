package org.bricolages.streaming;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.bricolages.streaming.s3.S3Agent;
import org.bricolages.streaming.s3.S3ObjectLocation;

import lombok.*;

@RequiredArgsConstructor
public class LocatorFactory {
    final S3Agent s3agent;

    SourceLocator parse(String urlString) throws URISyntaxException, IOException {
        val uri = new URI(urlString);
        val scheme = uri.getScheme();
        if (scheme == null) {
            return new LocalFileSourceLocator(urlString);
        } else if (scheme.equals("file")) {
            return new LocalFileSourceLocator(uri.getPath());
        } else if (scheme.equals("s3")) {
            return new S3ObjectSourceLocator(s3agent, new S3ObjectLocation(uri.getHost(), uri.getPath()));
        }
        throw new UnsupportedSchemeException("Unsupported scheme: " + scheme);
    }

    public static class UnsupportedSchemeException extends ApplicationError {
        static final long serialVersionUID = 1L;
        
        UnsupportedSchemeException(String message) {
            super(message);
        }
    }
}
