package gov.va.vba.vbms.kinesisexamplecommons.stream;

import java.util.List;

public interface Publisher<T, R> {
    R publish(List<T> data, PublishInfo info);
}
