package gov.va.vba.vbms.kinesissyncproducerexample.stream;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.va.vba.vbms.kinesisexamplecommons.stream.Event;
import gov.va.vba.vbms.kinesisexamplecommons.stream.PublishInfo;
import gov.va.vba.vbms.kinesisexamplecommons.stream.Publisher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
public class SDKPublisher implements Publisher<Event, List<PutRecordsResult>> {

    private final static int BATCH_SIZE = 50;
    private final static String AWS_REGION = "us-east-1";
    private final static String URL = "https://localhost:4568";
    private final AmazonKinesis kinesisClient = AmazonKinesisClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("", "")))
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(URL, AWS_REGION))
            .withClientConfiguration(new ClientConfiguration().withGzip(true))
            .build();
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Publishes {@link Event}s into AWS Kinesis.  This is done synchronously to preserve ordering.
     * @param data data to publish
     * @param info parameters around the AWS Kinesis streams
     * @return the {@link PutRecordsResult}s associated with the publishes
     */
    @Override
    public List<PutRecordsResult> publish(List<Event> data, PublishInfo info) {
        List<Event> chunk = new ArrayList<>();
        List<PutRecordsResult> results = new ArrayList<>();
        for (Event event : data) {
            chunk.add(event);
            if (chunk.size() == BATCH_SIZE) {
                handleChunk(chunk, results, info);
                chunk.clear();
            }
        }
        if (chunk.size() > 0) {
            handleChunk(chunk, results, info);
        }
        return results;
    }

    private void handleChunk(List<Event> chunk, List<PutRecordsResult> results, PublishInfo info) {
        PutRecordsRequest request = new PutRecordsRequest();
        request.setStreamName(info.getStreamName());
        request.setRecords(getEntries(chunk, info));
        createStreamIfMissing(info);
        results.add(kinesisClient.putRecords(request));
    }

    private void createStreamIfMissing(PublishInfo info) {
        if (!kinesisClient.listStreams().getStreamNames().contains(info.getStreamName())) {
            kinesisClient.createStream(info.getStreamName(), 1);
            log.info("stream " + info.getStreamName() + " created.");
        }
        log.info(info.getStreamName() + " state: " +
                kinesisClient.describeStream(info.getStreamName()).getStreamDescription().getStreamStatus());
    }

    private List<PutRecordsRequestEntry> getEntries(List<Event> chunk, PublishInfo info) {
        return chunk.stream()
                .map(event -> {
                    PutRecordsRequestEntry entry = new PutRecordsRequestEntry();
                    entry.setData(ByteBuffer.wrap(getJsonBytes(event)));
                    entry.setPartitionKey(info.getPartitionKey());
                    return entry;
                })
                .collect(Collectors.toList());
    }

    private byte[] getJsonBytes(Event event) {
        try {
            return objectMapper.writeValueAsBytes(event);
        } catch (JsonProcessingException jsonProcessingException) {
            throw new RuntimeException("error can't write data to json!", jsonProcessingException);
        }
    }
}
