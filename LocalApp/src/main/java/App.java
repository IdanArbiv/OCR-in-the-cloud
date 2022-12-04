import software.amazon.awssdk.services.sqs.model.Message;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Arrays;

public class App {


    final static AWS aws = AWS.getInstance();
    public static final String MANAGER_SERVICE_JAR = "/Users/galfadlon/IdeaProjects/MangerWorkersProject/out/artifacts/Manger_jar/Manger.jar";
    public static final String WORKER_JAR = "/Users/galfadlon/IdeaProjects/MangerWorkersProject/out/artifacts/Worker_jar/Worker.jar";
    public static final int MAX_PINGS_FOR_WAIT = 100;
    public static final String DELIMITER_1 = "delimiter_1";
    public static final String DELIMITER_2 = "delimiter_2";

    static String localKeyName;
    static String outputFilePath;
    static String inputFilePath;
    static int tasksPerWorker;
    static boolean shouldTerminate;
    static String managerInstanceID;


    public static void main(String[] args) {// args = [inFilePath, outFilePath, tasksPerWorker, -t (terminate, optional)]
        try {
            extractAndValidateArguments(args);
            setup();
            runLocalApp();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static void extractAndValidateArguments(String[] args) {
        if (args.length < 3) {
            System.out.println("[ERROR] Missing command line arguments.");
            System.exit(1);
        }
        inputFilePath = args[0];
        outputFilePath = args[1];
        tasksPerWorker = 0;
        try {
            String fileType = inputFilePath.substring(inputFilePath.indexOf('.') + 1);
            if (!fileType.equals("txt")) {
                System.out.println("[ERROR] Insert input text file.");
                System.exit(1);
            }
        } catch (Exception e) {
            System.out.println("[ERROR] Insert valid input file path, exception: " + e.getMessage());
            System.exit(1);
        }

        try {
            String fileType = outputFilePath.substring(outputFilePath.indexOf('.') + 1);
            if (!fileType.equals("html")) {
                System.out.println("[ERROR] Insert output html file.");
                System.exit(1);
            }
        } catch (Exception e) {
            System.out.println("[ERROR] Insert valid output file path, exception: " + e.getMessage());
            System.exit(1);
        }
        try {
            tasksPerWorker = Integer.parseInt(args[2]);
            if (tasksPerWorker <= 0 || tasksPerWorker > 15) {
                System.out.println("[ERROR] Tasks per worker must be a positive number between 0 to 15.");
                System.exit(1);
            }
        } catch (Exception e) {
            System.out.println("[ERROR] Tasks per worker is not a number, exception: " + e.getMessage());
            System.exit(1);
        }

        shouldTerminate = false;
        if (args.length > 3) {
            shouldTerminate = args[3].equals("-t");
        }
    }

    //Create Buckets, Create Queues, Upload JARs to S3
    private static void setup() {
        System.out.println("[DEBUG] Create bucket if not exist.");
        aws.createBucketIfNotExists(aws.bucketName);
        uploadJarsToS3();
        localKeyName = "localApp" + System.currentTimeMillis();
        createQueues();
    }

    private static void uploadJarsToS3() { // TODO - ADD CHECK IF JARS ALREADY EXIST
        System.out.println("[DEBUG] Upload Manager Jar to S3.");
        aws.uploadFileToS3(aws.bucketName, aws.managerJarKey, MANAGER_SERVICE_JAR);
        System.out.println("[DEBUG] Upload Worker Jar to S3.");
        aws.uploadFileToS3(aws.bucketName, aws.workerJarKey, WORKER_JAR);
        System.out.println("[DEBUG] Finish upload Jars to S3.");
    }

    //Creates all the queues
    private static void createQueues() {
        System.out.println("[DEBUG] Create queues if not exist.");
        aws.createMsgQueue(aws.managerQueueName);
        aws.createMsgQueue(aws.workerQueueName);
        aws.createMsgQueue(aws.localAppQueueName + localKeyName);
    }

    private static void runLocalApp() {
        System.out.println("[DEBUG] Create new Local Manager connection.");
        createManagerIfNotExists();
        sendNewFileTask();
        waitForResult();
        if (shouldTerminate)
            sendTerminateMessage();
        System.out.println("[INFO] Local app task is done. Output file in path: " + outputFilePath);
    }

    private static void createManagerIfNotExists() {
        System.out.println("[DEBUG] Create manager if not exist");
        if (!aws.checkIfInstanceExist(aws.managerTag)) {
            System.out.println("[DEBUG] Manager doesn't exist.");
            String managerScript = "#!/bin/bash\n" +
                    "echo Manager jar running\n" +
                    "echo s3://" + aws.bucketName + "/" + aws.managerJarKey + "\n" +
                    "mkdir ManagerFiles\n" +
                    "aws s3 cp s3://" + aws.bucketName + "/" + aws.managerJarKey + " ./ManagerFiles/" + aws.managerJarName + "\n" +
                    "echo manager copy the jar from s3\n" +
                    "java -jar /ManagerFiles/" + aws.managerJarName + "\n";

            managerInstanceID = aws.createInstance(managerScript, aws.managerTag, 1);
            System.out.println("[DEBUG] Manager created and started!.");
        } else {
            System.out.println("[DEBUG] Manager already exists and run.");
        }
    }

    private static void sendNewFileTask() {
        System.out.println("[DEBUG] Sending new file task");
        aws.uploadFileToS3(aws.bucketName, localKeyName, inputFilePath);
        System.out.println("[DEBUG] File task has send");
        aws.sendMessage(aws.getQueueUrl(aws.managerQueueName),
                createNewTaskMessage(localKeyName, tasksPerWorker));
    }

    private static String createNewTaskMessage(String keyName, int tasksPerWorker) {
        StringBuilder message = new StringBuilder();
        message.append(AWS.TaskType.NewTask.ordinal());
        message.append(DELIMITER_1);
        message.append(aws.bucketName);
        message.append(DELIMITER_1);
        message.append(keyName);
        message.append(DELIMITER_1);
        message.append(tasksPerWorker);
        return message.toString();
    }

    private static void waitForResult() {
        int howManyPingsSentWithoutAnswer = 0;
        while (true) {
            //TODO: think about starvation, Group ID
            System.out.println("Waiting for results, waiting 20sec");
            Message message = aws.getNewMessage(aws.getQueueUrl(aws.localAppQueueName + localKeyName));
            if (message != null) {
                String[] msgParts = message.body().split(DELIMITER_1); //[Type, Delimiter, Local Key, Delimiter, Bucket, Delimiter, S3 Key Name ]
                System.out.println("[INFO] Message for local app: " + Arrays.toString(msgParts));
                if (msgParts[1].equals(localKeyName)) {
                    if (Integer.parseInt(msgParts[0]) == AWS.TaskType.DoneTask.ordinal()) {
                        saveOutputToFile(aws.downloadFileFromS3(msgParts[2], msgParts[3]));
                        aws.deleteMessageFromQueue(aws.getQueueUrl(aws.localAppQueueName + localKeyName), message);
                        return;
                    } else { // Type - Ping
                        howManyPingsSentWithoutAnswer = 0;
                        System.out.println("[DEBUG] Local is reset number of ping without answer to: " + howManyPingsSentWithoutAnswer);
                    }
                }
            } else {
                if (howManyPingsSentWithoutAnswer > MAX_PINGS_FOR_WAIT) {
                    reinitializeManager();
                    break;
                }
                sendNewPingTask();
                System.out.println("[DEBUG] Number of pings without answer: " + howManyPingsSentWithoutAnswer);
                howManyPingsSentWithoutAnswer++;
            }
        }
    }

    private static void reinitializeManager() {
        System.out.println("[DEBUG] Reinitialize Manager");
        managerHardTermination();
        runLocalApp();

    }

    private static void managerHardTermination() {
        System.out.println("[DEBUG] Operate Hard Termination to Manager");
        if (managerInstanceID != null) aws.terminateInstance(managerInstanceID);
        else sendTerminateMessage();
    }

    private static void saveOutputToFile(String outputFileStr) {
        String htmlTagsOpen = "<!DOCTYPE html><html><body><p><table border=\"1\"><tr><th>Text</th><th>Image</th></tr>";
        String htmlTagsClose = "</table></p></body></html>";
        String[] outputLines = outputFileStr.split(DELIMITER_2);
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath));
            writer.write(htmlTagsOpen);
            for (String line : outputLines) {
                String[] singleOutput = line.split(DELIMITER_1); // singleOutput = [ImageUrl, convertedText]
                writer.write("<tr><th>" + singleOutput[1] + "</th><th>" + "<img src=\"" + singleOutput[0] + "\">" + "</th><th>");
            }
            writer.write(htmlTagsClose);
            writer.close();
            System.out.println("[DEBUG] Output has been updated");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void sendNewPingTask() {
        System.out.println("[DEBUG] Send Ping Task");
        aws.sendMessage(aws.getQueueUrl(aws.managerQueueName),
                createNewPingMessage());
    }

    private static String createNewPingMessage() {
        // Ping Type, Bucket Name, Local Key Name
        StringBuilder message = new StringBuilder();
        message.append(AWS.TaskType.Ping.ordinal());
        message.append(DELIMITER_1);
        message.append(localKeyName);
        return message.toString();
    }

    private static void sendTerminateMessage() {
        System.out.println("[DEBUG] Local " + localKeyName + " sending terminate message to manger!");
        aws.sendMessage(aws.getQueueUrl(aws.managerQueueName), String.valueOf(AWS.TaskType.Terminate.ordinal()));
    }


//    private static void test_setUp() {
//        String filePath = "C:\\Users\\idana\\Desktop\\input-example.txt";
//        int tasksPerWorker = 5;
//        //runLocalApp(filePath, tasksPerWorker, false);
//    }

//    private static void test_receivedMessage() {
//        Message m = aws.getNewMessage(aws.getQueueUrl(aws.managerQueueName));
//        System.out.println(m.body());
//        System.out.println(m.receiptHandle());
//        aws.deleteMessageFromQueue(aws.getQueueUrl(aws.managerQueueName), m);
//    }

//    private static void test_saveOutputToFile() {
//        aws.createBucketIfNotExists(aws.BUCKET_NAME);
//        outFilePath = "C:\\Users\\idan\\Desktop\\תואר\\סמסטר ז\\מערכות מבוזרות\\new1\\myapp\\src\\out\\output.html";
//        aws.uploadFileToS3(aws.BUCKET_NAME, "keytest", "C:\\Users\\דורין\\Desktop\\localAppQueueTest.txt");
//        String outputFileStr = aws.downloadFileFromS3(aws.BUCKET_NAME, "keytest");
//
//        String htmlTagsOpen = "<!DOCTYPE html><html><body><p><table><tr><th>Operation</th><th>Origin URL</th><th>Output URL</th></tr>";
//        String htmlTagsClose = "</table></p></body></html>";
//
//        String[] outputLines = outputFileStr.split("\n");
//        try {
//            BufferedWriter writer = new BufferedWriter(new FileWriter(outFilePath));
//            writer.write(htmlTagsOpen);
//            for (String line : outputLines) {
//                String[] singleOutput = line.split("\t");
//                writer.write("<tr><th>" + singleOutput[0] + "</th><th>" + singleOutput[1] + "</th><th>"
//                        + singleOutput[2] + "</th></tr>");
//            }
//            writer.write(htmlTagsClose);
//            writer.close();
//        } catch (Exception e) {
//            //catch any exceptions here
//        }
//    }
}
