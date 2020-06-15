package gov.va.vba.vbms.kinesisconsumerexample;

import com.amazonaws.SDKGlobalConfiguration;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class KinesisConsumerExampleApplication {

    /*
      needed for localstack
     */
    static {
        System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true");
        System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
        System.setProperty(SDKGlobalConfiguration.ACCESS_KEY_SYSTEM_PROPERTY, "testing123");
        System.setProperty(SDKGlobalConfiguration.SECRET_KEY_SYSTEM_PROPERTY, "testing123");
    }

    public static void main(String[] args) {
        SpringApplication.run(KinesisConsumerExampleApplication.class, args);
    }

    /**
     * Consumes records and logs them.  See
     * {@link gov.va.vba.vbms.kinesisconsumerexample.stream.PayloadStreamConfiguration} for more details
     * @param ctx context
     * @return commandlinerunner
     */
    @Bean
    public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
        return args -> {
        };
    }

}
