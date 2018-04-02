package org.bricolages.streaming;
import org.bricolages.streaming.filter.*;
import org.bricolages.streaming.event.*;
import org.bricolages.streaming.stream.*;
import org.bricolages.streaming.locator.*;
import org.bricolages.streaming.exception.*;
import org.bricolages.streaming.preflight.ReferenceGenerator;
import org.bricolages.streaming.preflight.Runner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.AmazonSQSClient;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
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

    String appName = "bricolage-preproc";

    public void run(String[] args) throws Exception {
        boolean oneshot = false;
        String mapUrl = null;
        String procUrl = null;
        String destUrl = null;
        String streamDefFilename = null;
        String schemaName = null;
        String tableName = null;
        String streamName = null;
        boolean checkOnly = false;
        boolean domainsReference = false;
        boolean dumpRoutes = false;

        for (int i = 0; i < args.length; i++) {
            if (Objects.equals(args[i], "--oneshot")) {
                oneshot = true;
            }
            else if (args[i].startsWith("--preflight=")) {
                streamDefFilename = getArg(args[i], "--preflight");
            }
            else if (args[i].equals("--check-only")) {
                checkOnly = true;
            }
            else if (args[i].startsWith("--schema-name=")) {
                schemaName = getArg(args[i], "--schema-name");
            }
            else if (args[i].startsWith("--table-name=")) {
                tableName = getArg(args[i], "--table-name");
            }
            else if (args[i].startsWith("--stream-name=")) {
                streamName = getArg(args[i], "--stream-name");
            }
            else if (args[i].startsWith("--map-url=")) {
                mapUrl = getArg(args[i], "--map-url");
            }
            else if (args[i].startsWith("--process-url=")) {
                procUrl = getArg(args[i], "--process-url");
            }
            else if (args[i].startsWith("--dest-url=")) {
                destUrl = getArg(args[i], "--dest-url");
            }
            else if (Objects.equals(args[i], "--types-reference")) {
                domainsReference = true;
            }
            else if (Objects.equals(args[i], "--dump-routes")) {
                dumpRoutes = true;
            }
            else if (Objects.equals(args[i], "--help")) {
                printUsage(System.out);
                System.exit(0);
            }
            else if (args[i].startsWith("-")) {
                errorExit("unknown option: " + args[i]);
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

        if (streamDefFilename != null) {   // preflight
            this.appName = "preflight";
            try {
                if (checkOnly) {
                    preflightRunner().loadFilter(streamDefFilename);
                }
                else {
                    if (procUrl == null) errorExit("--process-url is required");
                    if (destUrl == null) errorExit("--dest-url is required");
                    val src = S3ObjectLocator.parse(procUrl);
                    val dest = S3ObjectLocator.parse(destUrl);
                    val preflight = preflightRunner();
                    val filter = preflight.loadFilter(streamDefFilename);
                    preflight.preprocess(filter, src, dest);
                }
                createFlagFile(streamDefFilename);
            }
            catch (ApplicationError ex) {
                System.err.println("preflight: error: " + ex.getMessage());
                System.exit(1);
            }
        }
        else {   // preprocessor
            if (dumpRoutes) {
                val router = router();
                for (val ent : router.getEntries()) {
                    System.out.println(ent.description());
                }
                System.exit(0);
            }

            if (mapUrl != null) {
                val src = S3ObjectLocator.parse(mapUrl);
                val route = router().routeWithoutDB(src);
                if (route == null) {
                    errorExit("routing failed");
                }

                if (route.isBlackhole()) {
                    System.out.println("(blackhole)");
                }
                else {
                    System.out.println("streamName: " + route.getStreamName());

                    val bundle = route.getBundle();
                    System.out.println("streamBucket: " + bundle.getBucket());
                    System.out.println("streamPrefix: " + bundle.getPrefix());
                    System.out.println("destBucket: " + bundle.getDestBucket());
                    System.out.println("destPrefix: " + bundle.getDestPrefix());

                    System.out.println("objectPrefix: " + route.getObjectPrefix());
                    System.out.println("objectName: " + route.getObjectName());

                    System.out.println("destUrl: " + route.getDestLocator());
                }
                System.exit(0);
            }

            val preproc = preprocessor();
            if (procUrl != null) {
                val src = S3ObjectLocator.parse(procUrl);
                val out = new BufferedWriter(new OutputStreamWriter(System.out));
                val success = preproc.processUrl(src, out);
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
    }

    String getArg(String arg, String optName) {
        val kv = arg.split("=", 2);
        if (kv.length != 2) {
            errorExit("missing argument for " + optName);
        }
        return kv[1];
    }

    void errorExit(String msg) {
        System.err.println("error: " + msg);
        System.exit(1);
    }

    void printUsage(PrintStream s) {
        s.println("Usage: bricolage-streaming-preprocessor [options]");
        s.println("Options:");
        s.println("\t--oneshot             Process one ReceiveMessage and quit.");
        s.println("\t--map-url=S3URL       Prints destination S3 URL for S3URL and quit.");
        s.println("\t--process-url=S3URL   Process the data file S3URL as configured and print to stdout.");
        s.println("\t--dump-routes         Prints routing information and quit.");
        s.println("\t--help                Prints this message and quit.");
    }

    void createFlagFile(String streamDefFilename) throws IOException {
        val flagPath = Paths.get(streamDefFilename + ".ok");
        try (val f = Files.newBufferedWriter(flagPath)) {
            ;
        }
    }

    @Autowired
    Config config;

    @Bean
    public Runner preflightRunner() {
        return new Runner(filterFactory());
    }

    @Bean
    public Preprocessor preprocessor() {
        return new Preprocessor(eventQueue(), logQueue(), router());
    }

    @Bean
    public EventQueue eventQueue() {
        val config = this.config.getEventQueue();
        val sqs = new SQSQueue(AmazonSQSClientBuilder.defaultClient(), config.url);
        if (config.visibilityTimeout > 0) sqs.setVisibilityTimeout(config.visibilityTimeout);
        if (config.maxNumberOfMessages > 0) sqs.setMaxNumberOfMessages(config.maxNumberOfMessages);
        if (config.waitTimeSeconds > 0) sqs.setWaitTimeSeconds(config.waitTimeSeconds);
        return new EventQueue(sqs);
    }

    @Bean
    public LogQueue logQueue() {
        val config = this.config.getLogQueue();
        val sqs = new SQSQueue(AmazonSQSClientBuilder.defaultClient(), config.url);
        return new LogQueue(sqs);
    }

    @Bean
    public LocatorIOManager ioManager() {
        return new LocatorIOManager(AmazonS3ClientBuilder.defaultClient());
    }

    @Bean
    public PacketRouter router() {
        return new PacketRouter(this.config.getMappings());
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
}
