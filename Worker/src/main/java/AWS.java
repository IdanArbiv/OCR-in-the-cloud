import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.internal.util.EC2MetadataUtils;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;

public class AWS {
    private final S3Client s3;
    private final SqsClient sqs;
    private final Ec2Client ec2;


    public String managerQueueName = "ManagerQueue";
    public String workerQueueName = "WorkerQueue";

    public enum TaskType {
        NewTask,
        DoneTask,
        Ping,
        Terminate
    }


    public static Region region1 = Region.US_WEST_2;
    public static Region region2 = Region.US_EAST_1;

    private static final AWS instance = new AWS();

    private AWS() {
        s3 = S3Client.builder().region(region1).build();
        sqs = SqsClient.builder().region(region1).build();
        ec2 = Ec2Client.builder().region(region2).build();
    }

    public static AWS getInstance() {
        return instance;
    }


    public String getQueueUrl(String queueName) {
        while (true) {
            try {
                GetQueueUrlResponse getQueueUrlResponse = sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
                return getQueueUrlResponse.queueUrl();
            } catch (QueueDoesNotExistException ignored) {
            } catch (SqsException e) {
                System.err.println(e.awsErrorDetails().errorMessage());
                System.exit(1);
            }
        }
    }

    public void sendMessage(String queueUrl, String msg) {
        sqs.sendMessage(SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(msg)
                .delaySeconds(10)
                .build());
    }

    public Message getNewMessage(String queueUrl) {
        try {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .visibilityTimeout(80)
                    .maxNumberOfMessages(1) // TODO: CHECK IF NECESSARY
                    .waitTimeSeconds(20) //TODO CHECK IF NECESSARY
                    .build();
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).messages();
            if (messages.size() > 0) // return first message
                return messages.get(0);
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return null;
    }

    public void deleteMessageFromQueue(String queueUrl, Message message) {
        try {
            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build();
            sqs.deleteMessage(deleteMessageRequest);

        } catch (AwsServiceException e) {
            e.printStackTrace();
        }
    }

    public void shutdownInstance() {
        TerminateInstancesRequest request = TerminateInstancesRequest.builder()
                .instanceIds(EC2MetadataUtils.getInstanceId())
                .build();

        ec2.terminateInstances(request);
    }

    public void changeMessageVisibilityRequest(String queueUrl, String receiptHandle) {
        sqs.changeMessageVisibility(ChangeMessageVisibilityRequest.builder()
                .queueUrl(queueUrl)
                .visibilityTimeout(80)
                .receiptHandle(receiptHandle)
                .build());
    }
}
