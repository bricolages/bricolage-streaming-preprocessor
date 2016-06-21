package org.bricolages.streaming;

import com.amazonaws.auth.profile.*;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;
import java.io.*;

public class Preprocessor {
    static public void main(String[] args) throws Exception {
        new Preprocessor().download(args[0], args[1]);
    }

    void download(String bucket, String key) throws IOException {
        AmazonS3 s3 = new AmazonS3Client(new ProfileCredentialsProvider());
        S3Object obj = s3.getObject(new GetObjectRequest(bucket, key));
        InputStream s = obj.getObjectContent();
        System.out.println("OK");
        s.close();
    }
}
