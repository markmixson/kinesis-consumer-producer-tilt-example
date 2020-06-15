package gov.va.vba.vbms.kinesisproducerexample.stream;

import gov.va.vba.vbms.kinesisexamplecommons.stream.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.SerializationUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

@Component
@Slf4j
public class PayloadSource {
    private final BlockingQueue<Event> payloadEvent = new LinkedBlockingQueue<>();

    @Bean
    public Supplier<Event> producePayload() {
        return this.payloadEvent::poll;
    }

    /**
     * not actually used in this example because of batching issues. . . but left here
     * because it daemonizes the producer example.
     *
     * @param event event to add
     */
    public void sendPayload(Event event) {
        this.payloadEvent.offer(event);
        log.info("Event sent: " + event.toString());
    }
}
