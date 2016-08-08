package org.bricolages.streaming;
import org.bricolages.streaming.filter.ObjectFilterFactory;
import org.bricolages.streaming.event.EventQueue;
import org.bricolages.streaming.event.SQSQueue;
import org.bricolages.streaming.s3.S3Agent;
import org.bricolages.streaming.s3.ObjectMapper;
import org.bricolages.streaming.s3.S3ObjectLocation;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.sqs.AmazonSQSClient;
import java.util.Objects;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@EnableJpaRepositories
@Slf4j
public class Application {
    static public void main(String[] args) throws Exception {
        try (val ctx = SpringApplication.run(Application.class, args)) {
            ctx.getBean(Application.class).run(args);
        }
    }

    public void run(String[] args) throws Exception {
        boolean oneshot = false;
        String mapUrl = null;

        for (int i = 0; i < args.length; i++) {
            if (Objects.equals(args[i], "--oneshot")) {
                oneshot = true;
            }
            else if (args[i].startsWith("--map-url=")) {
                val kv = args[i].split("=", 2);
                if (kv.length != 2) {
                    System.err.println("missing argument for --map-url");
                    System.exit(1);
                }
                mapUrl = kv[1];
            }
            else if (args[i].startsWith("-")) {
                System.err.println("unknown option: " + args[i]);
                System.exit(1);
            }
            else {
                int argc = args.length - 1;
                if (argc > 1) {
                    System.err.println("too many arguments");
                    System.exit(1);
                }
                if (argc == 1) {
                    this.configPath = args[i];
                }
                break;
            }
        }
        if (mapUrl != null) {
            val result = mapper().map(S3ObjectLocation.forUrl(mapUrl));
            System.out.println(result.getDestLocation());
            System.exit(0);
        }

        log.info("configPath=" + configPath);
        this.preproc = preprocessor();
        if (oneshot) {
            preproc.runOneshot();
        }
        else {
            preproc.run();
        }
    }

    String configPath = "config/streaming-preprocessor.yml";
    Config config;

    // FIXME: replace by Spring DI
    Config getConfig() {
        if (this.config == null) {
            this.config = Config.load(configPath);
        }
        return this.config;
    }

    Preprocessor preproc;

    @Bean
    public Preprocessor preprocessor() {
        return new Preprocessor(eventQueue(), s3(), mapper(), filterFactory());
    }

    @Bean
    public EventQueue eventQueue() {
        val sqs = new SQSQueue(new AmazonSQSClient(), getConfig().queue.url);
        return new EventQueue(sqs);
    }

    @Bean
    public S3Agent s3() {
        return new S3Agent(new AmazonS3Client());
    }

    @Bean
    public ObjectMapper mapper() {
        return new ObjectMapper(getConfig().mapping);
    }

    @Bean
    public ObjectFilterFactory filterFactory() {
        return new ObjectFilterFactory();
    }
}
