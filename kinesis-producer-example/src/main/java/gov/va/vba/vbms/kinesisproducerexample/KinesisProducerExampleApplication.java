package gov.va.vba.vbms.kinesisproducerexample;

import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.va.vba.vbms.kinesisexamplecommons.data.Payload;
import gov.va.vba.vbms.kinesisexamplecommons.stream.Event;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.SerializationUtils;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

@SpringBootApplication
public class KinesisProducerExampleApplication {

    /*
      needed for localstack
    */
    static {
        System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true");
        System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
        System.setProperty(SDKGlobalConfiguration.ACCESS_KEY_SYSTEM_PROPERTY, "testing123");
        System.setProperty(SDKGlobalConfiguration.SECRET_KEY_SYSTEM_PROPERTY, "testing123");
    }

    private final static String ORIGINATOR = "KinesisProducerExample";
    private final static int TOTAL_MESSAGE_COUNT = 1000;
    private final static String MESSAGE_TYPE = "PAYLOAD";
    private final static String STREAM_NAME = "test_stream";
    private final static String PARTITION_KEY = "0";

    private final KinesisProducerExampleConfiguration config = new KinesisProducerExampleConfiguration();
    private final KinesisProducer kinesisProducer = new KinesisProducer(getKinesisProducerConfiguration());
    private final AtomicInteger count = new AtomicInteger();
    private final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        SpringApplication.run(KinesisProducerExampleApplication.class, args);
    }

    /**
     * Generates {@value #TOTAL_MESSAGE_COUNT} events via KPL
     * @param ctx the app context
     * @return commandlinerunner
     */
    @Bean
    public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
        return args -> {
            for (int i = 0; i < TOTAL_MESSAGE_COUNT; i++) {
                kinesisProducer.addUserRecord(STREAM_NAME, PARTITION_KEY,
                        ByteBuffer.wrap(objectMapper.writeValueAsBytes(getEvent())));
            }
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
        event.setSubject(payload);
        event.setOriginator(ORIGINATOR);
        event.setType(MESSAGE_TYPE);
        return event;
    }

    private KinesisProducerConfiguration getKinesisProducerConfiguration() {
        try {
            return config.kinesisProducerConfiguration();
        } catch (URISyntaxException e) {
            throw new RuntimeException("cannot get configuration!", e);
        }
    }
}
