package gov.va.vba.vbms.kinesisconsumerexample;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsync;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsyncClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClientBuilder;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Configuration to make localstack work for the KCL and Kinesis Binder
 */
@Configuration
public class KinesisConsumerExampleConfiguration {
    private final static String LOCALSTACK_EDGE_URL = "https://localhost:4566";
    private final static String LOCALSTACK_CLOUDWATCH_URL = "https://localhost:4582";
    private final static String AWS_REGION = "us-east-1";

    @Bean
    public AmazonKinesisAsync amazonKinesis() {
        return AmazonKinesisAsyncClientBuilder.standard()
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(LOCALSTACK_EDGE_URL, AWS_REGION))
                .build();
    }

    @Bean
    public AmazonDynamoDBAsync dynamoDB() {
        return AmazonDynamoDBAsyncClientBuilder.standard()
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(LOCALSTACK_EDGE_URL, AWS_REGION))
                .build();
    }

    @Bean
    public AmazonCloudWatchAsync cloudWatch() {
        return AmazonCloudWatchAsyncClientBuilder.standard()
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(LOCALSTACK_CLOUDWATCH_URL, AWS_REGION))
                .build();
    }

    @Bean
    public KinesisProducerConfiguration kinesisProducerConfiguration() throws URISyntaxException {
        URI kinesisUri = new URI(LOCALSTACK_EDGE_URL);
        return new KinesisProducerConfiguration()
                .setCredentialsProvider(new DefaultAWSCredentialsProviderChain())
                .setRegion(AWS_REGION)
                .setKinesisEndpoint(kinesisUri.getHost())
                .setKinesisPort(kinesisUri.getPort())
                .setVerifyCertificate(false);
    }
}
