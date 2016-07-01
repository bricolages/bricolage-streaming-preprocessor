package org.bricolages.streaming;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteMessageResult;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AbortedException;
import java.util.List;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.io.IOException;
import java.io.UncheckedIOException;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
class SQSQueue implements Iterable<Message> {
    final AmazonSQS sqs;
    final String queueUrl;
    int visibilityTimeout = 30;     // sec
    int maxNumberOfMessages = 10;   // max value
    int waitTimeSeconds = 20;       // max value; enables long poll

    public Stream<Message> stream() {
        return StreamSupport.stream(spliterator(), true);
    }

    public Iterator<Message> iterator() {
        return receiveMessages().iterator();
    }

    public List<Message> receiveMessages() {
        try {
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
        catch (AbortedException ex) {
            throw new ApplicationAbort(ex);
        }
        catch (AmazonClientException ex) {
            String msg = "receiveMessage failed: " + ex.getMessage();
            log.error(msg);
            throw new SQSException(msg);
        }
    }

    public void deleteMessage(String receiptHandle) {
        try {
            DeleteMessageRequest req = new DeleteMessageRequest(queueUrl, receiptHandle);
            log.info("deleteMessage queue={}, receiptHandle={}", queueUrl, receiptHandle);
            DeleteMessageResult res = sqs.deleteMessage(req);
        }
        catch (AmazonClientException ex) {
            String msg = "deleteMessage failed: " + ex.getMessage();
            log.error(msg);
            throw new SQSException(msg);
        }
    }

    // FIXME: batch delete
    //public void deleteMessages(List<String> receiptHandle) {}
}
