package gov.va.vba.vbms.kinesissyncproducerexample.stream;

import com.amazonaws.services.kinesis.AmazonKinesis;
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
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
public class SDKPublisher implements Publisher<Event, List<PutRecordsResult>> {

    private final static int BATCH_SIZE = 50;
    private final ObjectMapper objectMapper;
    private final AmazonKinesis amazonKinesis;

    /**
     * Synchronous Kinesis producer using the AWS SDK
     * @param objectMapper mapper to generate JSON data
     * @param amazonKinesis Kinesis Client
     */
    SDKPublisher(ObjectMapper objectMapper, AmazonKinesis amazonKinesis) {
        this.objectMapper = objectMapper;
        this.amazonKinesis = amazonKinesis;
    }

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
        PutRecordsRequest request = getPutRecordsRequest(chunk, info);
        createStreamIfMissing(info);
        PutRecordsResult chunkResults = amazonKinesis.putRecords(request);
        results.add(chunkResults);
        log.info("Produced " + chunkResults.getRecords().size() + " records of size "
                + request.getRecords().stream()
                .map(record -> record.getData().array().length)
                .reduce(0, Integer::sum) + " to the stream " + info.getStreamName()
                        + " with " + chunkResults.getFailedRecordCount() + " errors.");
    }

    private PutRecordsRequest getPutRecordsRequest(List<Event> chunk, PublishInfo info) {
        PutRecordsRequest request = new PutRecordsRequest();
        request.setStreamName(info.getStreamName());
        List<PutRecordsRequestEntry> entries = getEntries(chunk, info);
        request.setRecords(entries);
        return request;
    }

    private void createStreamIfMissing(PublishInfo info) {
        if (!amazonKinesis.listStreams().getStreamNames().contains(info.getStreamName())) {
            amazonKinesis.createStream(info.getStreamName(), 1);
            log.info("stream " + info.getStreamName() + " created.");
        }
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
