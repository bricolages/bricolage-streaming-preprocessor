package org.bricolages.streaming.preflight;
import org.bricolages.streaming.preflight.definition.*;
import org.bricolages.streaming.filter.ObjectFilterFactory;
import org.bricolages.streaming.filter.ObjectFilter;
import org.bricolages.streaming.filter.FilterResult;
import org.bricolages.streaming.locator.*;
import org.bricolages.streaming.exception.*;
import java.io.FileReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import lombok.*;

@RequiredArgsConstructor
public class Runner {
    final ObjectFilterFactory factory;

    public ObjectFilter loadFilter(String streamDefPath) throws IOException, LocatorIOException {
        val streamDef = loadStreamDef(streamDefPath);
        val generator = new ObjectFilterGenerator(factory, streamDef);
        return generator.generate();
    }

    StreamDefinitionEntry loadStreamDef(String path) throws IOException {
        val domainCollection = loadDomainCollection("config/domains.yml");
        val columnCollection = loadWellknownCollumnCollection("config/wellknown_columns.yml", domainCollection);
        val fileReader = new FileReader(path);
        return StreamDefinitionEntry.load(fileReader, domainCollection, columnCollection);
    }

    DomainCollection loadDomainCollection(String domainCollectionFilePath) throws IOException {
        try {
            val fileReader = new FileReader(domainCollectionFilePath);
            return DomainCollection.load(fileReader);
        } catch(FileNotFoundException ex) {
            return DomainCollection.empty();
        }
    }

    WellknownColumnCollection loadWellknownCollumnCollection(String wellknownColumnCollectionFilePath, DomainCollection domainCollection) throws IOException {
        try {
            val fileReader = new FileReader(wellknownColumnCollectionFilePath);
            return WellknownColumnCollection.load(fileReader, domainCollection);
        } catch(FileNotFoundException ex) {
            return WellknownColumnCollection.empty();
        }
    }

    public void preprocess(ObjectFilter filter, S3ObjectLocator src, S3ObjectLocator dest) throws IOException, LocatorIOException {
        System.err.printf("*** preproc start");
        System.err.printf("preproc source     : %s\n", src.toString());
        System.err.printf("preproc destination: %s\n", dest.toString());
        val result = new FilterResult(src.toString(), dest.toString());
        filter.processLocator(src, dest, result, src.toString());
        System.err.printf("*** preproc succeeded: in=%d, out=%d, error=%d\n", result.inputRows, result.outputRows, result.errorRows);
    }
}
