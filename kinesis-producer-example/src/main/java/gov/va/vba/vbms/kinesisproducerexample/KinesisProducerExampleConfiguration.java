package gov.va.vba.vbms.kinesisproducerexample;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClientBuilder;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Configuration to make localstack work for the KPL and Kinesis Binder
 */
@Configuration
public class KinesisProducerExampleConfiguration {

    private final static String AWS_REGION = "us-east-1";
    private final static String URL = "https://localhost:4568";

    @Bean
    public AmazonKinesisAsync amazonKinesis() {
        return AmazonKinesisAsyncClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(URL, AWS_REGION))
                .build();
    }

    @Bean
    public KinesisProducerConfiguration kinesisProducerConfiguration() throws URISyntaxException {
        URI kinesisUri = new URI(URL);
        return new KinesisProducerConfiguration()
                .setCredentialsProvider(new DefaultAWSCredentialsProviderChain())
                .setRegion(AWS_REGION)
                .setKinesisEndpoint(kinesisUri.getHost())
                .setKinesisPort(kinesisUri.getPort())
                .setVerifyCertificate(false);
    }
}
