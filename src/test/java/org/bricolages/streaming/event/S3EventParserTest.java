package org.bricolages.streaming.event;
import org.junit.Test;

import com.amazonaws.services.sqs.model.Message;

import static org.junit.Assert.*;
import lombok.*;

public class S3EventParserTest {
    @Test
    public void isBareS3Message_bare() throws Exception {
        val msgBody = "{\"Records\":[{\"eventVersion\":\"2.0\",\"eventSource\":\"bricolage:preprocessor\",\"eventTime\":\"2016-09-06T13:52:49Z\",\"eventName\":\"ObjectCreated:Put\",\"s3\":{\"s3SchemaVersion\":\"1.0\",\"bucket\":{\"name\":\"test-bucket\"},\"object\":{\"key\":\"test-prefix/test-file.json.gz\",\"size\":\"1358\",\"eTag\":\"bfa868a9c00376f6704f41d6a9e0da20\"}}}]}";
        val parser = new S3Event.Parser();
        assertEquals(
            true,
            parser.isBareS3Message(msgBody)
        );
    }

    @Test
    public void isSnsWrappedS3Message_wrapped() throws Exception {
        val msgBody = "{\"Type\":\"Notification\",\"MessageId\":\"d13256d1-86df-40d5-997f-06c205d6d1a0\",\"TopicArn\":\"arn:aws:sns:ap-northeast-1:123:hogehoge\",\"Subject\":\"Amazon S3 Notification\",\"Message\":\"{\\\"Records\\\":[{\\\"eventVersion\\\":\\\"2.0\\\",\\\"eventSource\\\":\\\"aws:s3\\\",\\\"awsRegion\\\":\\\"ap-northeast-1\\\",\\\"eventTime\\\":\\\"2017-06-14T08:17:01.647Z\\\",\\\"eventName\\\":\\\"ObjectCreated:Put\\\",\\\"userIdentity\\\":{\\\"principalId\\\":\\\"AWS:HOGEHOGE:hogehoge\\\"},\\\"requestParameters\\\":{\\\"sourceIPAddress\\\":\\\"127.0.0.1\\\"},\\\"responseElements\\\":{\\\"x-amz-request-id\\\":\\\"DEADBEEF\\\",\\\"x-amz-id-2\\\":\\\"aG9nZWhvZ2Vob2dlaG9nZWhvZ2VhCg==\\\"},\\\"s3\\\":{\\\"s3SchemaVersion\\\":\\\"1.0\\\",\\\"configurationId\\\":\\\"Zm9vYmFyYmF6aGhvZ2VodWdhcGkK\\\",\\\"bucket\\\":{\\\"name\\\":\\\"nanika-bucket\\\",\\\"ownerIdentity\\\":{\\\"principalId\\\":\\\"HOGEHOGE\\\"},\\\"arn\\\":\\\"arn:aws:s3:::hogehoge\\\"},\\\"object\\\":{\\\"key\\\":\\\"hoge.txt\\\",\\\"size\\\":10,\\\"eTag\\\":\\\"686897696a7c876b7e\\\",\\\"sequencer\\\":\\\"DEADBEEF\\\"}}}]}\",\"Timestamp\":\"2017-01-01T00:00:00.000Z\",\"SignatureVersion\":\"1\"}";
        val parser = new S3Event.Parser();
        assertEquals(
            true,
            parser.isSnsWrappedS3Message(msgBody)
        );
    }

    @Test
    public void parse_wrapped() throws Exception {
        val msgBody = "{\"Type\":\"Notification\",\"MessageId\":\"d13256d1-86df-40d5-997f-06c205d6d1a0\",\"TopicArn\":\"arn:aws:sns:ap-northeast-1:123:hogehoge\",\"Subject\":\"Amazon S3 Notification\",\"Message\":\"{\\\"Records\\\":[{\\\"eventVersion\\\":\\\"2.0\\\",\\\"eventSource\\\":\\\"aws:s3\\\",\\\"awsRegion\\\":\\\"ap-northeast-1\\\",\\\"eventTime\\\":\\\"2017-06-14T08:17:01.647Z\\\",\\\"eventName\\\":\\\"ObjectCreated:Put\\\",\\\"userIdentity\\\":{\\\"principalId\\\":\\\"AWS:HOGEHOGE:hogehoge\\\"},\\\"requestParameters\\\":{\\\"sourceIPAddress\\\":\\\"127.0.0.1\\\"},\\\"responseElements\\\":{\\\"x-amz-request-id\\\":\\\"DEADBEEF\\\",\\\"x-amz-id-2\\\":\\\"aG9nZWhvZ2Vob2dlaG9nZWhvZ2VhCg==\\\"},\\\"s3\\\":{\\\"s3SchemaVersion\\\":\\\"1.0\\\",\\\"configurationId\\\":\\\"Zm9vYmFyYmF6aGhvZ2VodWdhcGkK\\\",\\\"bucket\\\":{\\\"name\\\":\\\"nanika-bucket\\\",\\\"ownerIdentity\\\":{\\\"principalId\\\":\\\"HOGEHOGE\\\"},\\\"arn\\\":\\\"arn:aws:s3:::hogehoge\\\"},\\\"object\\\":{\\\"key\\\":\\\"hoge.txt\\\",\\\"size\\\":10,\\\"eTag\\\":\\\"686897696a7c876b7e\\\",\\\"sequencer\\\":\\\"DEADBEEF\\\"}}}]}\",\"Timestamp\":\"2017-01-01T00:00:00.000Z\",\"SignatureVersion\":\"1\"}";
        val parser = new S3Event.Parser();
        val msg = new Message();
        msg.setBody(msgBody);
        parser.parse(msg);
    }
}
