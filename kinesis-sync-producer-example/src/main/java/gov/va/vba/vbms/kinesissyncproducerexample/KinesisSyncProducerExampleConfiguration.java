package gov.va.vba.vbms.kinesissyncproducerexample;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KinesisSyncProducerExampleConfiguration {
    private final static String AWS_REGION = "us-east-1";
    private final static String KINESIS_URL = "https://localhost:4568";

    @Bean
    public AmazonKinesis amazonKinesis() {
        return AmazonKinesisClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(KINESIS_URL, AWS_REGION))
                .build();
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}
