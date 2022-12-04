
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;

import java.util.Arrays;

public class PingTaskHandler extends Thread {
    private final AWS aws = AWS.getInstance();
    private final PostmanService postmanService = PostmanService.getInstance();

    public PingTaskHandler() {
        System.out.println("[DEBUG] Ping Handler pool started...");
    }

    @Override
    public void run() {
        while (true) {
            Message pingTaskMessage = null;
            try {
                pingTaskMessage = postmanService.getPingTaskMessage();
                String[] msgParts = pingTaskMessage.body().split(PostmanService.DELIMITER_1); // [Type, Delimiter, localAppId]
                System.out.println("[DEBUG] Ping Handler pool found a message. The message is: " + Arrays.toString(msgParts));
                handlePingTask(msgParts);
            } catch (Exception e) { // In case of fail push the message back to sqs of Manger
                if (pingTaskMessage != null) {
                    System.out.println("[ERROR] Ping Handler pool got an exception and push the next message back to the sqs: " + pingTaskMessage.body() + " " + e.getMessage());
                    aws.sendMessage(aws.getQueueUrl(aws.managerQueueName), pingTaskMessage.body());
                }
            }

        }

    }

    private void handlePingTask(String[] msgParts) {
        String localAppId = msgParts[1];
        try {
            aws.sendMessage(aws.getQueueUrl(aws.localAppQueueName + localAppId),
                    createPingAnswerMessage(localAppId));
        } catch (Exception e) {
            System.out.println("[ERROR] " + e.getMessage());
        }
    }

    private String createPingAnswerMessage(String localAppId) {
        StringBuilder message = new StringBuilder();
        message.append(AWS.TaskType.Ping.ordinal());
        message.append(PostmanService.DELIMITER_1);
        message.append(localAppId);
        System.out.println("[DEBUG] Ping pool handler send the next Message to workers sqs: \n" + message);
        return message.toString();
    }

}
