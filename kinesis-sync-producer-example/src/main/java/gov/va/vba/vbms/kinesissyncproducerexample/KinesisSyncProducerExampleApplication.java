package gov.va.vba.vbms.kinesissyncproducerexample;

import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import gov.va.vba.vbms.kinesisexamplecommons.data.Payload;
import gov.va.vba.vbms.kinesisexamplecommons.stream.Event;
import gov.va.vba.vbms.kinesisexamplecommons.stream.PublishInfo;
import gov.va.vba.vbms.kinesisexamplecommons.stream.Publisher;
import gov.va.vba.vbms.kinesissyncproducerexample.stream.SDKPublisher;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.RandomStringUtils;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@SpringBootApplication
public class KinesisSyncProducerExampleApplication {

    /*
      needed for localstack
    */
    static {
        System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true");
        System.setProperty(SDKGlobalConfiguration.AWS_EC2_METADATA_DISABLED_SYSTEM_PROPERTY, "true");
        System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
        System.setProperty(SDKGlobalConfiguration.ACCESS_KEY_SYSTEM_PROPERTY, "testing123");
        System.setProperty(SDKGlobalConfiguration.SECRET_KEY_SYSTEM_PROPERTY, "testing123");
    }

    private final static String ORIGINATOR = "KinesisProducerExample";
    private final static int TOTAL_MESSAGE_COUNT = 1000;
    private final static String MESSAGE_TYPE = "PAYLOAD";
    private final static String STREAM_NAME = "test_stream";
    private final static String PARTITION_KEY = "0";

    private final AtomicInteger count = new AtomicInteger();
    private final Publisher<Event, List<PutRecordsResult>> publisher;

    KinesisSyncProducerExampleApplication(SDKPublisher publisher) {
        this.publisher = publisher;
    }

    public static void main(String[] args) {
        SpringApplication.run(KinesisSyncProducerExampleApplication.class, args);
    }

    /**
     * Generates {@value #TOTAL_MESSAGE_COUNT} events through the AWS SDK
     * @param ctx the app context
     * @return commandlinerunner
     */
    @Bean
    public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
        return args -> {
            List<Event> events = new ArrayList<>();
            for (int i = 0; i < TOTAL_MESSAGE_COUNT; i++) {
                events.add(getEvent());
            }
            publisher.publish(events, new PublishInfo(STREAM_NAME, PARTITION_KEY));
        };
    }

    /**
     * Generates an event with a gradually scaling payload size
     * @return an event
     */
    private Event getEvent() {
        Payload payload = new Payload();
        payload.setId(RandomStringUtils.randomAlphanumeric(count.incrementAndGet() * 100));
        Event event = new Event();
        event.setId(UUID.randomUUID());
        event.setSubject(payload);
        event.setOriginator(ORIGINATOR);
        event.setType(MESSAGE_TYPE);
        return event;
    }
}


