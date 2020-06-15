package gov.va.vba.vbms.kinesisconsumerexample.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import gov.va.vba.vbms.kinesisexamplecommons.stream.Event;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Slf4j
@Component
public class PayloadStreamConfiguration {

    private final AtomicInteger count = new AtomicInteger();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Bean
    public Consumer<Flux<Message<List<byte[]>>>> processPayload() {
        return messageFlux -> messageFlux
                .map(e -> getEvents(e.getPayload()))
                .doOnNext(events -> events.forEach(event -> log.info(
                        "Event read - payload size: " + event.getSubject().getId().length()  +
                                " count number: " + count.incrementAndGet())))
                .subscribe();
    }

    private List<Event> getEvents(List<byte[]> events) {
        return events.stream()
                .map(data -> {
                    try {
                        return objectMapper.readValue(data, Event.class);
                    } catch (IOException ioException) {
                        throw new RuntimeException("can't record event!", ioException);
                    }
                })
                .collect(Collectors.toList());
    }
}
