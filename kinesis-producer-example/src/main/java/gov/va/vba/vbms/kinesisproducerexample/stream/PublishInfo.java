package gov.va.vba.vbms.kinesisproducerexample.stream;

import lombok.Data;

@Data
public class PublishInfo {
    private final String streamName;
    private final String partitionKey;
}
