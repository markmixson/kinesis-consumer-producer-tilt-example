# Synchronous Producer Example using AWS Kinesis

This example uses only the AWS SDK to produce to an AWS Stream.

There are a number of problems with this example, including not adjusting batch size based on payload size.  Kinesis 
has strict limits on what request sizes can be.  Also, the example doesn't handle communication to different shards.