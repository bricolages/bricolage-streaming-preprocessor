package org.bricolages.streaming.locator;
import org.bricolages.streaming.s3.S3Agent;
import org.bricolages.streaming.s3.S3ObjectLocation;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import lombok.*;

@RequiredArgsConstructor
public class LocatorFactory {
    final S3Agent s3agent;

    public SourceLocator parse(String urlString) throws URISyntaxException, IOException {
        val uri = new URI(urlString);
        val scheme = uri.getScheme();
        if (scheme == null) {
            // eg. "./relatice/path/to/file.gz"
            return new LocalFileSourceLocator(urlString);
        }
        else if (scheme.equals("file")) {
            return new LocalFileSourceLocator(uri.getPath());
        }
        else if (scheme.equals("s3")) {
            return new S3ObjectSourceLocator(s3agent, new S3ObjectLocation(uri.getHost(), uri.getPath().replaceFirst("^/", "")));
        }
        throw new UnsupportedSchemeException("Unsupported scheme: " + scheme);
    }
}
