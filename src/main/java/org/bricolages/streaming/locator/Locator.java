package org.bricolages.streaming.locator;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import lombok.*;

@RequiredArgsConstructor
abstract public class Locator {
    static public Locator parse(String urlString) throws LocatorParseException {
        try {
            val uri = new URI(urlString);
            val scheme = uri.getScheme();
            if (scheme == null) {
                // eg. "./relatice/path/to/file.gz"
                return new LocalFileLocator(urlString);
            }
            else if (scheme.equals("file")) {
                return new LocalFileLocator(uri.getPath());
            }
            else if (scheme.equals("s3")) {
                return new S3ObjectLocator(uri.getHost(), uri.getPath().replaceFirst("^/", ""));
            }
            else {
                throw new LocatorParseException("Unsupported scheme: " + scheme);
            }
        }
        catch (URISyntaxException | IOException ex) {
            throw new LocatorParseException(ex);
        }
    }

    abstract public String toString();
    abstract public boolean isLocalFile();
    abstract public boolean isS3Object();
    abstract public S3ObjectLocator asS3ObjectLocator();
}
