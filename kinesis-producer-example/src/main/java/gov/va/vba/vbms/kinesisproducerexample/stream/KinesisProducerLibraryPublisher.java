package gov.va.vba.vbms.kinesisproducerexample.stream;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ListenableFuture;
import gov.va.vba.vbms.kinesisexamplecommons.stream.Event;
import gov.va.vba.vbms.kinesisexamplecommons.stream.PublishInfo;
import gov.va.vba.vbms.kinesisexamplecommons.stream.Publisher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
public class KinesisProducerLibraryPublisher implements Publisher<Event, List<ListenableFuture<UserRecordResult>>> {

    private final KinesisProducer kinesisProducer;
    private final ObjectMapper objectMapper;

    KinesisProducerLibraryPublisher(KinesisProducer kinesisProducer, ObjectMapper objectMapper) {
        this.kinesisProducer = kinesisProducer;
        this.objectMapper = objectMapper;
    }

    @Override
    public List<ListenableFuture<UserRecordResult>> publish(List<Event> data, PublishInfo info) {
        List<ListenableFuture<UserRecordResult>> results = data.stream()
                .map(event -> {
                    ByteBuffer byteBuffer = ByteBuffer.wrap(getSerializedEvent(event));
                    return kinesisProducer.addUserRecord(info.getStreamName(), info.getPartitionKey(), byteBuffer);
                })
                .collect(Collectors.toList());
        kinesisProducer.flushSync();
        log.info("finished writing " + results.size() + " events to stream");
        return results;
    }

    private byte[] getSerializedEvent(Event event) {
        try {
            return objectMapper.writeValueAsBytes(event);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("can't build serialized event!", e);
        }
    }
}
