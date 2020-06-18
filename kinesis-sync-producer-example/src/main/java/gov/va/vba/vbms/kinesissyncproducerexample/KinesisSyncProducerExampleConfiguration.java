package gov.va.vba.vbms.kinesissyncproducerexample;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.cloudformation.AmazonCloudFormationAsync;
import com.amazonaws.services.cloudformation.AmazonCloudFormationAsyncClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class KinesisSyncProducerExampleConfiguration {
    private final static String AWS_REGION = "us-east-1";
    private final static String KINESIS_URL = "https://localhost:4568";
    private final static String EDGE_URL = "https://localhost:4566";

    @Bean
    public AmazonKinesis amazonKinesis() {
        return AmazonKinesisClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(KINESIS_URL, AWS_REGION))
                .build();
    }

    @Bean
    public AmazonCloudFormationAsync amazonCloudFormation() {
        return AmazonCloudFormationAsyncClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(EDGE_URL, AWS_REGION))
                .build();
    }

    @Bean
    @Primary
    public AWSCredentialsProvider buildDefaultAWSCredentialsProvider() {
        return new DefaultAWSCredentialsProviderChain();
    }
}
