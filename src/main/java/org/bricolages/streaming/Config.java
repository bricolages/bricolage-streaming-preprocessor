package org.bricolages.streaming;
import org.bricolages.streaming.s3.ObjectMapper;
import org.yaml.snakeyaml.Yaml;
import java.util.List;
import java.util.Map;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.IOException;

class Config {
    static public Config load(String path) throws ConfigError {
        try {
            try (InputStream in = new FileInputStream(path)) {
                return loadFromStream(in);
            }
        }
        catch (IOException ex) {
            throw new ConfigError(ex);
        }
    }

    static public Config loadResource(String name) throws ConfigError {
        try {
            try (InputStream in = ClassLoader.getSystemResourceAsStream(name)) {
                return loadFromStream(in);
            }
        }
        catch (IOException ex) {
            throw new ConfigError(ex);
        }
    }

    static public Config loadFromStream(InputStream in) throws IOException {
        return new Yaml().loadAs(in, Config.class);
    }

    public QueueEntry queue;
    public List<ObjectMapper.Entry> mapping;

    static final class QueueEntry {
        public String url;
        public int visibilityTimeout;
        public int maxNumberOfMessages;
        public int waitTimeSeconds;
    }
}
