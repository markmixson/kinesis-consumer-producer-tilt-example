package gov.va.vba.vbms.kinesisexamplecommons.data;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
public class Payload implements Serializable {
    private String id;
}
