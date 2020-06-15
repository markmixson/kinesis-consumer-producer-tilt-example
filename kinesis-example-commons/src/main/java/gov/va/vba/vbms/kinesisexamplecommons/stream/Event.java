package gov.va.vba.vbms.kinesisexamplecommons.stream;

import gov.va.vba.vbms.kinesisexamplecommons.data.Payload;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.UUID;

@Data
@NoArgsConstructor
public class Event implements Serializable {
    private UUID id;
    private Payload subject;
    private String type;
    private String originator;
}
