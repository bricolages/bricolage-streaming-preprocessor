package org.bricolages.streaming;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.event.*;
import java.io.*;
import java.nio.file.*;
import java.nio.charset.Charset;
import lombok.*;

public class Preprocessor {
    static public void main(String[] args) throws Exception {
        //S3ObjectLocation loc = new S3ObjectLocation(args[0], args[1]);
        //S3ObjectLocation dest = new S3ObjectLocation(args[2], args[3]);
        Preprocessor pp = new Preprocessor();
        //pp.download(loc);
        //pp.s3filter(loc, dest);
        pp.receiveQueue();
    }

    final EventQueue eventQueue;
    final S3Agent s3;
    final ObjectFilter filter = new ObjectFilter();

    public Preprocessor() {
        super();
        String queueUrl = "****";
        AWSCredentialsProvider credentials = new ProfileCredentialsProvider();
        this.eventQueue = new EventQueue(new SQSQueue(credentials, queueUrl));
        this.s3 = new S3Agent(credentials);
    }

    void receiveQueue() throws IOException {
        eventQueue.events().forEach(e -> {
            System.out.println("object: " + e.getLocation());
            System.out.println("size  : " + e.getObjectSize());
        });
    }

    void download(S3ObjectLocation loc) throws IOException {
        s3.download(loc, loc.basename());
    }

    void s3filter(S3ObjectLocation src, S3ObjectLocation dest) throws IOException {
        try (BufferedWriter w = Files.newBufferedWriter(dest.basename(), Charset.defaultCharset())) {
            try (BufferedReader r = s3.openBufferedReader(src)) {
                filter.apply(r, w);
            }
        }
    }
}
