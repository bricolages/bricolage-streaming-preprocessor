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

class SQSQueue implements Iterable<Message> {
    final AWSCredentialsProvider credentials;
    final String queueUrl;
    final AmazonSQS sqs;

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
                    this.iter = receiveQueue().listIterator();
                }
                catch (IOException ex) {
                    // FIXME: retry, log
                    throw new UncheckedIOException(ex);
                }
            }
            return iter.next();
        }
    }

    static final int SQS_VISIBILITY_TIMEOUT = 30;   // sec

    public List<Message> receiveQueue() throws IOException {
        String queueUrl = "https://sqs.ap-northeast-1.amazonaws.com/789035092620/log-stream-dev";
        ReceiveMessageRequest req = new ReceiveMessageRequest(queueUrl);
        req.setVisibilityTimeout(SQS_VISIBILITY_TIMEOUT);
        req.setMaxNumberOfMessages(10);   // max value
        req.setWaitTimeSeconds(20);       // max value; enables long poll
        ReceiveMessageResult res = sqs.receiveMessage(req);
        return res.getMessages();
    }

    public void deleteMessage(String receiptHandle) {
        DeleteMessageRequest req = new DeleteMessageRequest(queueUrl, receiptHandle);
        DeleteMessageResult res = sqs.deleteMessage(req);
        // FIXME: check result
    }

    // FIXME: batch delete
}
