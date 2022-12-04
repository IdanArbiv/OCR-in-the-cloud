
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.Arrays;

public class LocalTaskHandler extends Thread {
    AWS aws = AWS.getInstance();
    private final PostmanService postmanService = PostmanService.getInstance();


    public LocalTaskHandler() {
        System.out.println("[DEBUG] Local Handler pool started...");
    }

    @Override
    public void run() {
        while (true) {
            Message localTaskMessage = null;
            try {
                localTaskMessage = postmanService.getLocalTaskMessage();
                String[] msgParts = localTaskMessage.body().split(PostmanService.DELIMITER_1); // [Type, Delimiter, Bucket Name, Delimiter,Bucket Key Name, Delimiter, Tasks Per Worker]
                System.out.println("[DEBUG] Local Handler pool found a message. The message is: " + Arrays.toString(msgParts));
                handleNewLocalTask(msgParts);
            } catch (Exception ignore) { // In case of fail push the message back to sqs of Manger
                if (localTaskMessage != null) {
                    System.out.println("[DEBUG] Local Handler pool got an exception and push the next message back to the sqs: " + localTaskMessage.body());
                    aws.sendMessage(aws.getQueueUrl(aws.managerQueueName), localTaskMessage.body());
                }
            }
        }
    }

    private void handleNewLocalTask(String[] msgParts) {
        // msgParts = [Type, Delimiter, Bucket Name, Delimiter, localAppId, Delimiter, Tasks Per Worker]
        String bucketName = msgParts[1];
        String localAppId = msgParts[2];
        int tasksPerWorker = Integer.parseInt(msgParts[3]);
        String[] lines = getFileLines(bucketName, localAppId);
        int numberOfTasks = lines.length;
        // Update global fields of postman and updating number of workers
        synchronized (postmanService.getUpdateWorkersLock()) { // Synchronized on a specific lock
            // The next 2 lines is for calculating updating fields in the postman and then calculating with them hoe many workers need ti crete/delete
            postmanService.addNumberOfTasksPerWorkerForAllLocalApps(tasksPerWorker);
            int RemainingTasksTotal = postmanService.addNumberOfRemainingTasksTotal(numberOfTasks);
            System.out.println("[DEBUG-COUNTERS] NUMBER OF Of Remaining Tasks Total: " + RemainingTasksTotal);
            postmanService.updateNumberOfWorkers(); // Creating new workers if needed
        }
        LocalAppData currentLocalApp = new LocalAppData(numberOfTasks, tasksPerWorker);
        postmanService.getLocalApps().putIfAbsent(localAppId, currentLocalApp);
        createTasksForWorkers(lines, localAppId);
    }

    private void createTasksForWorkers(String[] lines, String localAppId) {
        for (int lineNumber = 0; lineNumber < lines.length; lineNumber++)
            aws.sendMessage(aws.getQueueUrl(aws.workerQueueName),
                    createWorkerTaskMsg(lines[lineNumber], localAppId, lineNumber));
    }

    private String createWorkerTaskMsg(String imageURL, String localAppId, int index) {
        StringBuilder message = new StringBuilder();
        message.append(AWS.TaskType.NewTask.ordinal());
        message.append(PostmanService.DELIMITER_1);
        message.append(localAppId);
        message.append(PostmanService.DELIMITER_1);
        message.append(imageURL);
        message.append(PostmanService.DELIMITER_1);
        message.append(index);
        System.out.println("[DEBUG] Local pool handler send the next Message to workers sqs: \n" + message);
        return message.toString();
    }

    private String[] getFileLines(String bucketName, String localAppId) {
        String fileContent = aws.downloadFileFromS3(bucketName, localAppId);
        return fileContent.split(String.valueOf('\n')); // Number of lines === Number of jumping a line in the file
    }


}
