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
import java.util.Base64;
import java.util.List;

public class AWS {
    private static final int MAX_INSTANCES = 19;
    private final S3Client s3;
    private final SqsClient sqs;
    private final Ec2Client ec2;


    public String managerQueueName = "ManagerQueue";
    public String workerQueueName = "WorkerQueue";
    public String localAppQueueName = "LocalAppQueue";
    public String managerTag = "Manager";
    public String workerTag = "Worker";

    public String managerJarKey = "Manager1";
    public String workerJarKey = "Worker1";

    public String managerJarName = "ManagerProject.jar";
    public String workerJarName = "WorkerProject.jar";

    public SqsClient getSqs() {
        return sqs;
    }

    public enum TaskType {
        NewTask,
        DoneTask,
        Ping,
        Terminate;
    }


    public static String ami = "ami-00e95a9222311e8ed";
    public static Region region1 = Region.US_WEST_2;
    public static Region region2 = Region.US_EAST_1;

    public String bucketName = "bucket1638974297770";
    public String keyName = "key" + System.currentTimeMillis();

    private static final AWS instance = new AWS();

    private AWS() {
        s3 = S3Client.builder().region(region1).build();
        sqs = SqsClient.builder().region(region1).build();
        ec2 = Ec2Client.builder().region(region2).build();
    }

    public static AWS getInstance() {
        return instance;
    }


    //S3
    public void createBucketIfNotExists(String bucketName) {
        try {
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucketName)
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                    .locationConstraint(BucketLocationConstraint.US_WEST_2)
                                    .build())
                    .build());
            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
        } catch (S3Exception e) {
            System.out.println(e.getMessage());
        }
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

    public static void cleanUp(S3Client s3Client, String bucketName, String keyName) {
        System.out.println("Cleaning up...");
        try {
            System.out.println("Deleting object: " + keyName);
            DeleteObjectRequest deleteObjectRequest =
                    DeleteObjectRequest.builder().bucket(bucketName).key(keyName).build();
            s3Client.deleteObject(deleteObjectRequest);
            System.out.println(keyName + " has been deleted.");
            System.out.println("Deleting bucket: " + bucketName);
            DeleteBucketRequest deleteBucketRequest =
                    DeleteBucketRequest.builder().bucket(bucketName).build();
            s3Client.deleteBucket(deleteBucketRequest);
            System.out.println(bucketName + " has been deleted.");
            System.out.printf("%n");
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        System.out.println("Cleanup complete");
        System.out.printf("%n");
    }

    //sqs
    public void createMsgQueue(String queueName) {
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();
        sqs.createQueue(createQueueRequest);
    }

    public String getQueueUrl(String queueName) {
        while (true) {
            try {
                GetQueueUrlResponse getQueueUrlResponse = sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
                String queueUrl = getQueueUrlResponse.queueUrl();
                return queueUrl;
            } catch (QueueDoesNotExistException e) {
                continue;
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
                .delaySeconds(10) //TODO: CHECK IF ITS NECESSARY. זה מעכב את ההודעה 10 שניות עד שהיא נכנסת לתור ואז אף אחד לא יכול לראות אותה.
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

    //ec2
    public boolean checkIfInstanceExist(String tagName) {
        int a = getNumOfInstances(tagName);
        System.out.println("[DEBUG] Number of " + tagName + " instances: " + a);
        return a > 0;
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


    public String createInstance(String script, String tagName, int numberOfInstances) {
//        Ec2Client ec2 = Ec2Client.create();

        RunInstancesRequest runRequest = (RunInstancesRequest) RunInstancesRequest.builder()
                .instanceType(InstanceType.M4_LARGE)
                .imageId(ami)
                .maxCount(numberOfInstances)
                .minCount(1)
                .keyName("vockey")
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                .userData(Base64.getEncoder().encodeToString((script).getBytes()))
                .build();


        RunInstancesResponse response = ec2.runInstances(runRequest);

        String instanceId = response.instances().get(0).instanceId();

        software.amazon.awssdk.services.ec2.model.Tag tag = Tag.builder()
                .key("Name")
                .value(tagName)
                .build();

        CreateTagsRequest tagRequest = (CreateTagsRequest) CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);
            System.out.printf(
                    "[DEBUG] Successfully started EC2 instance %s based on AMI %s\n",
                    instanceId, ami);

        } catch (Ec2Exception e) {
            System.err.println("[ERROR] " + e.getMessage());
            System.exit(1);
        }
        return instanceId;
    }

    public void shutdownInstance() {
        TerminateInstancesRequest request = TerminateInstancesRequest.builder()
                .instanceIds(EC2MetadataUtils.getInstanceId())
                .build();

        ec2.terminateInstances(request);
    }

    public void terminateInstance(String instanceId) {
        TerminateInstancesRequest request = TerminateInstancesRequest.builder()
                .instanceIds(instanceId)
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
