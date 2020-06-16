package gov.va.vba.vbms.kinesisproducerexample.stream;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
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
import java.util.concurrent.atomic.AtomicLong;
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
        AtomicLong count = new AtomicLong(0L);
        List<ListenableFuture<UserRecordResult>> results = data.stream()
                .map(event -> getResult(event, count, info))
                .collect(Collectors.toList());
        kinesisProducer.flushSync();
        log.info("finished writing " + results.size() + " events of size " + count.get() + " total bytes to stream");
        return results;
    }

    private ListenableFuture<UserRecordResult> getResult(Event event, AtomicLong count, PublishInfo info) {
        byte[] serializedEvent = getSerializedEvent(event);
        ByteBuffer byteBuffer = ByteBuffer.wrap(serializedEvent);
        count.addAndGet(byteBuffer.array().length);
        return kinesisProducer.addUserRecord(info.getStreamName(), info.getPartitionKey(), byteBuffer);
    }

    private byte[] getSerializedEvent(Event event) {
        try {
            return objectMapper.writeValueAsBytes(event);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("can't build serialized event!", e);
        }
    }
}
