package org.bricolages.streaming;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
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
        this.configPath = args[0];
        log.info("configPath=" + configPath);
        this.preproc = preprocessor();
        preproc.run();
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
        return new Preprocessor(eventQueue(), s3(), mapper(), filter());
    }

    @Bean
    public EventQueue eventQueue() {
        val sqs = sqs();
        sqs.maxNumberOfMessages = 3;   // FIXME
        return new EventQueue(sqs);
    }

    @Bean
    public SQSQueue sqs() {
        return new SQSQueue(awsCredentials(), getConfig().queue.url);
    }

    @Bean
    public S3Agent s3() {
        return new S3Agent(awsCredentials());
    }

    @Bean
    public AWSCredentialsProvider awsCredentials() {
        return new ProfileCredentialsProvider();
    }

    @Bean
    public ObjectMapper mapper() {
        return new ObjectMapper(getConfig().mapping);
    }

    @Bean
    public ObjectFilter filter() {
        return new ObjectFilter();
    }
}
