import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.internal.util.EC2MetadataUtils;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class AWS {
    private final S3Client s3;
    private final SqsClient sqs;
    private final Ec2Client ec2;


    public String managerQueueName = "ManagerQueue";
    public String workerQueueName = "WorkerQueue";
    public String localAppQueueName = "LocalAppQueue";
    public String workerTag = "Worker";

    public String workerJarKey = "Worker1";

    public String workerJarName = "WorkerProject.jar";

    public enum TaskType {
        NewTask,
        DoneTask,
        Ping,
        Terminate
    }


    public static String ami = "ami-00e95a9222311e8ed";
    public static Region region1 = Region.US_WEST_2;
    public static Region region2 = Region.US_EAST_1;
    public String bucketName = "bucket1638974297771";

    private static final AWS instance = new AWS();

    private AWS() {
        s3 = S3Client.builder().region(region1).build();
        sqs = SqsClient.builder().region(region1).build();
        ec2 = Ec2Client.builder().region(region2).build();
    }

    public static AWS getInstance() {
        return instance;
    }


    public void uploadFileToS3(String bucketName, String keyName, String filePath) {
        s3.putObject(PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(keyName)
                        .build(),
                RequestBody.fromFile(new File(filePath)));
    }

    public String downloadFileFromS3(String bucketName, String keyName) {
        try {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(keyName)
                    .build();
            ResponseBytes<GetObjectResponse> res = s3.getObjectAsBytes(getObjectRequest);
            return res.asUtf8String();
        } catch (AwsServiceException e) {
            e.printStackTrace();
        }
        return null;
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
                    .maxNumberOfMessages(1)
                    .waitTimeSeconds(20)
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

    public void deleteBucket(String bucketName) {
        ListObjectsRequest listRequest = ListObjectsRequest.builder()
                .bucket(bucketName).build();

        ListObjectsResponse listResponse = s3.listObjects(listRequest);
        List<S3Object> listObjects = listResponse.contents();

        List<ObjectIdentifier> objectsToDelete = new ArrayList<>();

        for (S3Object s3Object : listObjects) {
            objectsToDelete.add(ObjectIdentifier.builder().key(s3Object.key()).build());
        }

        DeleteObjectsRequest deleteObjectsRequest = DeleteObjectsRequest.builder()
                .bucket(bucketName)
                .delete(Delete.builder().objects(objectsToDelete).build())
                .build();

        DeleteObjectsResponse deleteObjectsResponse = s3.deleteObjects(deleteObjectsRequest);

        System.out.println("[DEBUG] Objects deleted: " + deleteObjectsResponse.hasDeleted());

        DeleteBucketRequest request = DeleteBucketRequest.builder().bucket(bucketName).build();

        s3.deleteBucket(request);

        System.out.println("[DEBUG] Bucket " + bucketName + " deleted.");
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

    public int getNumOfInstances(String tagName) {
        String nextToken = null;
        int counter = 0;

        do {
            DescribeInstancesRequest request = DescribeInstancesRequest
                    .builder().nextToken(nextToken).build();
            DescribeInstancesResponse response = ec2.describeInstances(request);

            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    if (instance.state().name().toString().equals("running")) {
                        for (software.amazon.awssdk.services.ec2.model.Tag tag : instance.tags()) {
                            if (tag.key().equals("Name") && tag.value().equals(tagName)) {
                                counter++;
                            }
                        }
                    }
                }
            }
            nextToken = response.nextToken();

        } while (nextToken != null);

        return counter;
    }


    public void createInstance(String script, String tagName, int numberOfInstances) {
        try {
            RunInstancesRequest runRequest = RunInstancesRequest.builder()
                    .instanceType(InstanceType.M4_LARGE)
                    .imageId(ami)
                    .maxCount(numberOfInstances)
                    .minCount(1)
                    .keyName("vockey")
                    .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                    .userData(Base64.getEncoder().encodeToString((script).getBytes()))
                    .build();

            Tag tag = Tag.builder()
                    .key("Name")
                    .value(tagName)
                    .build();


            RunInstancesResponse response = ec2.runInstances(runRequest);

            Collection<String> instanceIDs = response.instances().stream().map(Instance::instanceId).collect(Collectors.toList());
            CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                    .resources(instanceIDs)
                    .tags(tag)
                    .build();

            try {
                ec2.createTags(tagRequest);
                System.out.printf(
                        "[DEBUG] Successfully started EC2 instance %s based on AMI %s\n",
                        instanceIDs, ami);

            } catch (Ec2Exception e) {
                System.err.println("[ERROR] " + e.getMessage());
            }
        }
        catch (Exception e) {
            System.out.println("[ERROR] " + e.getMessage());
        }

    }

    public void deleteQueue(String queueUrl) {
        try {
            DeleteQueueRequest deleteMessageRequest = DeleteQueueRequest.builder()
                    .queueUrl(queueUrl)
                    .build();
            sqs.deleteQueue(deleteMessageRequest);

        } catch (AwsServiceException e) {
            e.printStackTrace();
        }
    }

    public void shutdownInstance() {
        try {
            TerminateInstancesRequest request = TerminateInstancesRequest.builder()
                    .instanceIds(EC2MetadataUtils.getInstanceId())
                    .build();

            ec2.terminateInstances(request);
        } catch (Exception e) {
            System.out.println("[ERROR] " + e.getMessage());
        }
    }

}
