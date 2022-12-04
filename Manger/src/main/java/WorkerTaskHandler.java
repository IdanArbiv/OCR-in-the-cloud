
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collection;

public class WorkerTaskHandler extends Thread {
    private final AWS aws = AWS.getInstance();
    private final PostmanService postmanService = PostmanService.getInstance();

    public WorkerTaskHandler() {
        System.out.println("[DEBUG] Worker Handler pool started...");
    }

    @Override
    public void run() {
        while (true) {
            Message workerTaskMessage = null;
            try {
                workerTaskMessage = postmanService.getWorkerTaskMessage();
                String[] msgParts = workerTaskMessage.body().split(PostmanService.DELIMITER_1);
                // [Type, Delimiter, localAppId, Delimiter, originUrl, Delimiter, output(converts text), Delimiter, index of url in the file(number of line)]
                System.out.println("[DEBUG] Worker Handler pool found a message. The message is: " + Arrays.toString(msgParts));
                handleWorkerTaskMessage(msgParts);
            } catch (Exception ignore) { // In case of fail push the message back to sqs of Manger
                if (workerTaskMessage != null) {
                    System.out.println("[DEBUG] Worker Handler pool got an exception and push the next message back to the sqs: " + workerTaskMessage.body());
                    aws.sendMessage(aws.getQueueUrl(aws.managerQueueName), workerTaskMessage.body());
                }
            }
        }
    }

    private void handleWorkerTaskMessage(String[] msgParts) {
        String localAppId = msgParts[1];
        String originImgUrl = msgParts[2];
        String convertedText = msgParts[3];
        String index = msgParts[4];
        String convertedTextAngOriginalURL = originImgUrl + PostmanService.DELIMITER_1 + convertedText + PostmanService.DELIMITER_2;
        try {
            LocalAppData currentLocalApp = postmanService.getLocalApps().get(localAppId);
            currentLocalApp.putIfAbsent(index, convertedTextAngOriginalURL); // Take current local app  and add to his data hash map (<int-index, String- output for specific image>) the output
            int numberOfRemainTask;
            numberOfRemainTask = postmanService.getLocalApps().get(localAppId).atomicDecrementTasksCounter(); // Decrease by one
            int remainingTasksTotal = postmanService.decrementRemainingTasksTotal(); // Decrease by one
            System.out.println("[DEBUG-COUNTERS] NUMBER OF Of Remaining Tasks Total: " + remainingTasksTotal);
            System.out.println("[DEBUG] LocalApp id: " + localAppId + ", Number of remain tasks: " + numberOfRemainTask);
            if (numberOfRemainTask == 0) {  // No more missions so sending the answer to the local up
                synchronized (postmanService.getUpdateWorkersLock()) {
                    int tasksPerWorker = currentLocalApp.getNumberOfTasksPerWorker();
                    postmanService.decrementTasksPerWorkerForAllLocalApps(tasksPerWorker * -1);
                    System.out.println("[DEBUG-COUNTERS] NUMBER OF Of Tasks Per Worker For All Local Apps: " + remainingTasksTotal);
                    finishLocalAppTasks(localAppId);
                }
            }

        } catch (Exception e) {
            System.out.println("[DEBUG] Already done handling localApp id: " + localAppId);
        }
    }

    private void finishLocalAppTasks(String localAppId) {
        createAndSendOutputFileToLocal(localAppId);
        synchronized (postmanService.getUpdateWorkersLock()) {
            postmanService.updateNumberOfWorkers();
        }
        postmanService.getLocalApps().remove(localAppId);

    }

    private void createAndSendOutputFileToLocal(String localAppId) {
        String outputContent = getOutputFileContent(localAppId);
        String outputPath = "./" + localAppId + ".txt";
        try {
            PrintWriter pw = new PrintWriter(outputPath);
            pw.print(outputContent);
            pw.close();
            String s3KeyName = "outputs/" + localAppId + ".txt";
            aws.uploadFileToS3(aws.bucketName, s3KeyName, outputPath);
            aws.sendMessage(aws.getQueueUrl(aws.localAppQueueName + localAppId),
                    createLocalAppMsg(localAppId, aws.bucketName, s3KeyName));
            new File(outputPath).delete();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private String getOutputFileContent(String localAppId) {
        StringBuilder outputContent = new StringBuilder();
        Collection<String> allLocalsConvertedText = postmanService.getLocalApps().get(localAppId).getTasksStatus().values();
        for (String convertsTextAndUrl : allLocalsConvertedText) {
            outputContent.append(convertsTextAndUrl);
        }
        return outputContent.toString();
    }

    private String createLocalAppMsg(String localAppId, String bucketName, String s3KeyName) {
        StringBuilder message = new StringBuilder();
        message.append(AWS.TaskType.DoneTask.ordinal());
        message.append(PostmanService.DELIMITER_1);
        message.append(localAppId);
        message.append(PostmanService.DELIMITER_1);
        message.append(bucketName);
        message.append(PostmanService.DELIMITER_1);
        message.append(s3KeyName);
        System.out.println("[DEBUG] Worker pool handler send the next Message to local app: \n" + message);
        return message.toString();
    }
}
