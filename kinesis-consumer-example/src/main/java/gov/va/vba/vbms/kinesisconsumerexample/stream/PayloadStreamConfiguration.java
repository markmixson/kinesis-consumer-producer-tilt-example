package gov.va.vba.vbms.kinesisconsumerexample.stream;

import com.amazonaws.services.kinesis.model.Record;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.SerializationUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import gov.va.vba.vbms.kinesisexamplecommons.stream.Event;
import reactor.core.publisher.Flux;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

@Slf4j
@Component
public class PayloadStreamConfiguration {

    private final AtomicInteger count = new AtomicInteger();

    @Bean
    public Consumer<Flux<List<Record>>> processPayload() {
        return recordFlux -> recordFlux
                .flatMap(Flux::fromIterable)
                .map(this::getEvents)
                .doOnNext(events -> events.forEach(event -> log.info(
                        "Event read - payload size: " + event.getSubject().getId().length()  +
                                " count number: " + count.incrementAndGet())))
                .subscribe();
    }

    @SuppressWarnings("unchecked")
    private List<Event> getEvents(Record record) {
        Object object = SerializationUtils.deserialize(record.getData().array());
        if (object instanceof Event) {
            return Collections.singletonList((Event) object);
        } else {
            return (List<Event>) object;
        }
    }
}
