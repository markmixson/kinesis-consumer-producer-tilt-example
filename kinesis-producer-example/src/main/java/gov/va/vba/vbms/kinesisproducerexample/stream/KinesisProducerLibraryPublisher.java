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
import gov.va.vba.vbms.kinesisproducerexample.KinesisProducerExampleConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
public class KinesisProducerLibraryPublisher implements Publisher<Event, List<ListenableFuture<UserRecordResult>>> {

    private final KinesisProducerExampleConfiguration config = new KinesisProducerExampleConfiguration();
    private final KinesisProducer kinesisProducer = new KinesisProducer(getKinesisProducerConfiguration());
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public List<ListenableFuture<UserRecordResult>> publish(List<Event> data, PublishInfo info) {
        List<ListenableFuture<UserRecordResult>> results = new ArrayList<>();
        for (Event event : data) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(getSerializedEvent(event));
            ListenableFuture<UserRecordResult> future =
                    kinesisProducer.addUserRecord(info.getStreamName(), info.getPartitionKey(), byteBuffer);
            results.add(future);
        }
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

    private KinesisProducerConfiguration getKinesisProducerConfiguration() {
        try {
            return config.kinesisProducerConfiguration();
        } catch (URISyntaxException e) {
            throw new RuntimeException("cannot get configuration!", e);
        }
    }
}
