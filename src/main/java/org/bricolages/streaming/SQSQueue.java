package org.bricolages.streaming;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteMessageResult;
import java.util.List;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.io.IOException;
import java.io.UncheckedIOException;
import lombok.*;
import lombok.extern.java.Log;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class SQSQueue implements Iterable<Message> {
    final AWSCredentialsProvider credentials;
    final String queueUrl;
    final AmazonSQS sqs;
    int visibilityTimeout = 30;     // sec
    int maxNumberOfMessages = 10;   // max value
    int waitTimeSeconds = 20;       // max value; enables long poll

    public SQSQueue(AWSCredentialsProvider credentials, String queueUrl) {
        super();
        this.queueUrl = queueUrl;
        this.credentials = credentials;
        this.sqs = new AmazonSQSClient(credentials);
    }

    public Stream<Message> stream() {
        return StreamSupport.stream(spliterator(), true);
    }

    public Iterator<Message> iterator() {
        return new MessageIterator();
    }

    final class MessageIterator implements Iterator<Message> {
        ListIterator<Message> iter = null;

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public Message next() {
            while (iter == null || !iter.hasNext()) {
                try {
                    this.iter = receiveMessages().listIterator();
                }
                catch (IOException ex) {
                    // FIXME: retry, log
                    throw new UncheckedIOException(ex);
                }
            }
            return iter.next();
        }
    }

    public List<Message> receiveMessages() throws IOException {
        ReceiveMessageRequest req = new ReceiveMessageRequest(queueUrl);
        req.setVisibilityTimeout(visibilityTimeout);
        req.setMaxNumberOfMessages(maxNumberOfMessages);
        req.setWaitTimeSeconds(waitTimeSeconds);
        log.info("receiveMessage queue={}, visibilityTimeout={}, maxNumberOfMessages={}, waitTimeSeconds={}",
            queueUrl, visibilityTimeout, maxNumberOfMessages, waitTimeSeconds);
        ReceiveMessageResult res = sqs.receiveMessage(req);
        log.debug("receiveMessage returned");
        return res.getMessages();
    }

    public void deleteMessage(String receiptHandle) {
        DeleteMessageRequest req = new DeleteMessageRequest(queueUrl, receiptHandle);
        log.info("deleteMessage queue={}, receiptHandle={}", queueUrl, receiptHandle);
        DeleteMessageResult res = sqs.deleteMessage(req);
        // FIXME: check result
    }

    // FIXME: batch delete
    //public void deleteMessages(List<String> receiptHandle) {}
}
