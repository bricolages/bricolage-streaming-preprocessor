package org.bricolages.streaming.preflight;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.io.FileNotFoundException;
import org.apache.commons.io.FilenameUtils;
import org.bricolages.streaming.Config;
import org.bricolages.streaming.preflight.definition.*;
import org.bricolages.streaming.stream.*;
import org.bricolages.streaming.stream.processor.StreamColumnProcessor;
import org.bricolages.streaming.filter.*;
import org.bricolages.streaming.locator.*;
import org.bricolages.streaming.exception.*;
import lombok.*;

@RequiredArgsConstructor
public class Runner {
    final ObjectFilterFactory factory;
    final PacketRouter router;
    final Config config;

    private DomainCollection loadDomainCollection(String domainCollectionFilePath) throws IOException {
        try {
            val fileReader = new FileReader(domainCollectionFilePath);
            return DomainCollection.load(fileReader);
        } catch(FileNotFoundException ex) {
            return DomainCollection.empty();
        }
    }

    private WellknownColumnCollection loadWellknownCollumnCollection(String wellknownColumnCollectionFilePath, DomainCollection domainCollection) throws IOException {
        try {
            val fileReader = new FileReader(wellknownColumnCollectionFilePath);
            return WellknownColumnCollection.load(fileReader, domainCollection);
        } catch(FileNotFoundException ex) {
            return WellknownColumnCollection.empty();
        }
    }

    private StreamDefinitionEntry loadStreamDef(StreamDefinitionFile streamDefFile) throws IOException {
        val domainCollection = loadDomainCollection("config/domains.yml");
        val columnCollection = loadWellknownCollumnCollection("config/wellknown_columns.yml", domainCollection);
        val fileReader = new FileReader(streamDefFile.getFilepath());
        return StreamDefinitionEntry.load(fileReader, domainCollection, columnCollection);
    }

    private void saveLoadJob(StreamDefinitionFile streamDefFile, S3ObjectLocator dest, String fullTableName) throws IOException {
        val path = streamDefFile.getLoadJobFilepath();
        System.err.printf("generating load job: %s\n", path.toString());
        try (val loadJobFile = new FileOutputStream(path)) {
            new LoadJobSerializer(loadJobFile).serialize(dest, streamDefFile.getCreateTableFilepath(), fullTableName, config.getSrcDs(), config.getDestDs());
        }
    }

    static class StreamDefinitionFile {
        @Getter
        private final String filepath;
        private final String filepathWithoutExt;

        StreamDefinitionFile(String filepath) {
            if (!FilenameUtils.isExtension(filepath, "yml") && !FilenameUtils.isExtension(filepath, "strdef")) {
                throw new IllegalArgumentException("extension of stream definition file must be 'yml' or 'strdef'");
            }
            this.filepath = filepath;
            this.filepathWithoutExt = FilenameUtils.removeExtension(filepath);
        }

        String getCreateTableFilepath() {
            return this.filepathWithoutExt + ".ct";
        }


        String getLoadJobFilepath() {
            return this.filepathWithoutExt + "_preflight_load_.job";
        }
    }

    public void generateAndPreprocess(String streamDefPath, S3ObjectLocator src, String tableSpec) throws IOException, LocatorIOException {
        val route = router.routeWithoutDB(src);
        if (route == null) {
            throw new ConfigError("log object routing failed: " + src);
        }
        val streamName = route.getStreamName();
        val dest = route.getDestLocator();

        val processors = generateDefs(streamDefPath, streamName, dest, tableSpec);

        preprocess(processors, streamName, src, dest);
    }

    public void generateWithRouting(String streamDefPath, S3ObjectLocator src, String tableSpec) throws IOException, LocatorIOException {
        val route = router.routeWithoutDB(src);
        if (route == null) {
            throw new ConfigError("log object routing failed: " + src);
        }
        val streamName = route.getStreamName();
        val dummyDest = new S3ObjectLocator("dummy-bucket", "dummy-key");
        generateDefs(streamDefPath, streamName, dummyDest, tableSpec);
    }

    public void generateWithoutRouting(String streamDefPath, String streamName, String tableSpec) throws IOException, LocatorIOException {
        val dummyDest = new S3ObjectLocator("dummy-bucket", "dummy-key");
        generateDefs(streamDefPath, streamName, dummyDest, tableSpec);
    }

    ObjectFilter generateDefs(String streamDefPath, String streamName, S3ObjectLocator dest, String tableSpec) throws IOException, LocatorIOException {
        val streamDefFile = new StreamDefinitionFile(streamDefPath);
        val streamDef = loadStreamDef(streamDefFile);

        val generator = new ObjectFilterGenerator(factory, streamDef);
        val filter = generator.generate();

        saveLoadJob(streamDefFile, dest, tableSpec);

        return filter;
    }

    void preprocess(ObjectFilter filter, String streamName, S3ObjectLocator src, S3ObjectLocator dest) throws IOException, LocatorIOException {
        System.err.printf("*** preproc start");
        System.err.printf("preproc source     : %s\n", src.toString());
        System.err.printf("preproc destination: %s\n", dest.toString());
        val result = new FilterResult(src.toString(), dest.toString());
        filter.processLocator(src, dest, result, streamName);
        System.err.printf("*** preproc succeeded: in=%d, out=%d, error=%d\n", result.inputRows, result.outputRows, result.errorRows);
    }
}
