package org.bricolages.streaming;
import org.bricolages.streaming.filter.ObjectFilterFactory;
import org.bricolages.streaming.filter.OpBuilder;
import org.bricolages.streaming.preflight.ReferenceGenerator;
import org.bricolages.streaming.preflight.Runner;
import org.bricolages.streaming.event.EventQueue;
import org.bricolages.streaming.event.LogQueue;
import org.bricolages.streaming.event.SQSQueue;
import org.bricolages.streaming.s3.S3Agent;
import org.bricolages.streaming.s3.ObjectMapper;
import org.bricolages.streaming.s3.S3ObjectLocation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.sqs.AmazonSQSClient;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.util.Objects;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@EnableJpaRepositories
@EnableTransactionManagement
@Slf4j
@EnableConfigurationProperties(Config.class)
public class Application {
    static public void main(String[] args) throws Exception {
        try (val ctx = SpringApplication.run(Application.class, args)) {
            ctx.getBean(Application.class).run(args);
        }
    }

    public void run(String[] args) throws Exception {
        boolean oneshot = false;
        SourceLocator mapUrl = null;
        SourceLocator procUrl = null;
        String streamDefFilename = null;
        String schemaName = null;
        String tableName = null;
        boolean domainsReference = false;

        for (int i = 0; i < args.length; i++) {
            if (Objects.equals(args[i], "--oneshot")) {
                oneshot = true;
            }
            else if (args[i].startsWith("--preflight=")) {
                val kv = args[i].split("=", 2);
                if (kv.length != 2) {
                    System.err.println("missing stream definition file for --preflight");
                    System.exit(1);
                }
                streamDefFilename = kv[1];
            }
            else if (args[i].startsWith("--schema-name=")) {
                val kv = args[i].split("=", 2);
                if (kv.length != 2) {
                    System.err.println("missing stream definition file for --schema-name");
                    System.exit(1);
                }
                schemaName = kv[1];
            }
            else if (args[i].startsWith("--table-name=")) {
                val kv = args[i].split("=", 2);
                if (kv.length != 2) {
                    System.err.println("missing stream definition file for --table-name");
                    System.exit(1);
                }
                tableName = kv[1];
            }
            else if (args[i].startsWith("--map-url=")) {
                val kv = args[i].split("=", 2);
                if (kv.length != 2) {
                    System.err.println("missing argument for --map-url");
                    System.exit(1);
                }
                mapUrl = locatorFactory().parse(kv[1]);
            }
            else if (args[i].startsWith("--process-url=")) {
                val kv = args[i].split("=", 2);
                if (kv.length != 2) {
                    System.err.println("missing argument for --process-url");
                    System.exit(1);
                }
                procUrl = locatorFactory().parse(kv[1]);
            }
            else if (Objects.equals(args[i], "--domains-reference")) {
                domainsReference = true;
            }
            else if (Objects.equals(args[i], "--help")) {
                printUsage(System.out);
                System.exit(0);
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
                break;
            }
        }

        if (mapUrl != null) {
            val result = mapper().map(mapUrl.toString());
            System.out.println(result.getDestLocation());
            System.exit(0);
        }

        val preproc = preprocessor();
        if (streamDefFilename != null) {
            if (procUrl == null) {
                System.err.println("missing argument: --process-url");
                System.exit(1);
            }
            if (schemaName == null) {
                System.err.println("missing argument: --schema-name");
                System.exit(1);
            }
            if (tableName == null) {
                System.err.println("missing argument: --table-name");
                System.exit(1);
            }
            preflightRunner().run(streamDefFilename, procUrl, schemaName, tableName);
        }
        else if (procUrl != null) {
            val out = new BufferedWriter(new OutputStreamWriter(System.out));
            val success = preproc.processUrl(procUrl, out);
            out.flush();
            if (success) {
                System.err.println("SUCCEEDED");
                System.exit(0);
            }
            else {
                System.err.println("FAILED");
                System.exit(1);
            }
        }
        else if (oneshot) {
            preproc.runOneshot();
        }
        else if (domainsReference) {
            new ReferenceGenerator().generate();
        }
        else {
            preproc.run();
        }
    }

    void printUsage(PrintStream s) {
        s.println("Usage: bricolage-streaming-preprocessor [options]");
        s.println("Options:");
        s.println("\t--oneshot             Process one ReceiveMessage and quit.");
        s.println("\t--map-url=S3URL       Prints destination S3 URL for S3URL and quit.");
        s.println("\t--process-url=S3URL   Process the data file S3URL as configured and print to stdout.");
        s.println("\t--help                Prints this message and quit.");
    }

    @Autowired
    Config config;

    @Bean
    public Runner preflightRunner() {
        return new Runner(preprocessor(), filterFactory(), s3(), mapper(), config);
    }

    @Bean
    public Preprocessor preprocessor() {
        return new Preprocessor(eventQueue(), logQueue(), s3(), mapper(), filterFactory());
    }

    @Bean
    public EventQueue eventQueue() {
        val config = this.config.getEventQueue();
        val sqs = new SQSQueue(new AmazonSQSClient(), config.url);
        if (config.visibilityTimeout > 0) sqs.setVisibilityTimeout(config.visibilityTimeout);
        if (config.maxNumberOfMessages > 0) sqs.setMaxNumberOfMessages(config.maxNumberOfMessages);
        if (config.waitTimeSeconds > 0) sqs.setWaitTimeSeconds(config.waitTimeSeconds);
        return new EventQueue(sqs);
    }

    @Bean
    public LogQueue logQueue() {
        val config = this.config.getLogQueue();
        val sqs = new SQSQueue(new AmazonSQSClient(), config.url);
        return new LogQueue(sqs);
    }

    @Bean
    public S3Agent s3() {
        return new S3Agent(new AmazonS3Client());
    }

    @Bean
    public ObjectMapper mapper() {
        return new ObjectMapper(this.config.getMappings());
    }

    @Bean
    public ObjectFilterFactory filterFactory() {
        return new ObjectFilterFactory();
    }

    @Autowired
    SequencialNumberRepository sequentialNumberRepository;

    @Bean
    public OpBuilder opBuilder() {
        return new OpBuilder(sequentialNumberRepository);
    }

    @Bean
    public LocatorFactory locatorFactory() {
        return new LocatorFactory(s3());
    }
}
