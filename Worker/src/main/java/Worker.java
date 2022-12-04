import com.asprise.ocr.Ocr;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.util.Arrays;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

public class Worker {
    static final AWS aws = AWS.getInstance();
    public final static String DELIMITER_1 = "delimiter_1";

    public static void main(String[] args) throws IOException { // TODO: CHECK ABOUT STATIC
        AtomicBoolean finishedWork = new AtomicBoolean(false); // for visibility extend
        receiveMessagesProcessAndSendAnswerToManager(finishedWork);
    }

    private static void receiveMessagesProcessAndSendAnswerToManager(AtomicBoolean finishedWork) {
        while (true) {
            System.out.println("[DEBUG] Worker is looking for messages...");
            String workerQueueUrl = aws.getQueueUrl(aws.workerQueueName);
            Message message = aws.getNewMessage(workerQueueUrl);
            if (message != null) { // If any message exit in the sqs queue
                System.out.println("[DEBUG] Worker found a massage and his extending visibility time");
                finishedWork.set(false); // For beginning of the process
                makeMessageVisibilityDynamic(message, workerQueueUrl, finishedWork);
                String[] msgParts = message.body().split(DELIMITER_1);
                System.out.println("[DEBUG] Worker msg:" + Arrays.toString(msgParts));// msgParts = [type, Delimiter, localAppId, Delimiter, Image URL, Delimiter index(num of line in the original urls file)]
                if (shouldTerminate(msgParts[0])) {
                    terminateWorker(workerQueueUrl, message);
                    return;
                }
                String localAppId = msgParts[1];
                String url = msgParts[2];
                String index = msgParts[3];
                System.out.println("[DEBUG] Worker converting image to text");
                String outputMessage = convertImageToText(url);
                if (outputMessage.equals(""))  // There is a bug in the ocr package that sometimes returns an empty string
                    outputMessage = "Fail To Convert The Image";
                System.out.println("[DEBUG] Worker sending massage to manager and delete massage form the queue");
                aws.sendMessage(aws.getQueueUrl(aws.managerQueueName), createDoneTaskMessage(localAppId, url, outputMessage, index));
                aws.deleteMessageFromQueue(aws.getQueueUrl(aws.workerQueueName), message);
                finishedWork.set(true); // Because finished the process
            }
        }
    }

    private static void terminateWorker(String workerQueueUrl, Message message) {
        System.out.println("[DEBUG] Worker is shutting down.");
        aws.deleteMessageFromQueue(workerQueueUrl, message);
//        System.exit(0); // For local debugging
        aws.shutdownInstance();
    }

    private static boolean shouldTerminate(String msgType) {
        return Integer.parseInt(msgType) == AWS.TaskType.Terminate.ordinal();
    }

    private static void makeMessageVisibilityDynamic(Message message, String workerQueueUrl, AtomicBoolean finishedWork) {
        String receiptHandle = message.receiptHandle(); // Needed for making message visibility time dynamic
        Thread timerThread = new Thread(() -> {
            Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    if (!finishedWork.get()) // Worker need more time so extend visibility time for the message
                        aws.changeMessageVisibilityRequest(workerQueueUrl, receiptHandle);
                    else {
                        timer.cancel();
                    }
                }
            }, 40, 80 * 1000); // Delay before start interval is zero. Means that every 80 second the run mission is executed
        }); //TODO CHANGE ATOMIC BOOL IF FINISH
        timerThread.start();
    }

//     For tesseract ocr package
//    private static String convertImageToText(String url) {
//        try {
//            Tesseract tesseract = new Tesseract();
//            tesseract.setDatapath("src/main/resources/tessdata");
//            URL imageURL = new URL(url);
//            URLConnection conn = imageURL.openConnection();
//            String contentType = conn.getContentType(); // For example: image/png OR image/gif OR image/jpeg
//            String[] imageTypes = contentType.split(Pattern.quote("/")); // e.g: [image, png]
//            if (imageTypes[0].equals("image")) { // The url is valid!
//                BufferedImage img = ImageIO.read(imageURL);
//                String textFromImage = tesseract.doOCR(img); // The converted text from the image
//                System.out.println("[DEBUG] Worker OCR Answer: \n Original URL: " + url + "\nOutput text: \n" + textFromImage);
//                return textFromImage;
//            } else // In case that the url isn't image URL.
//                return "Invalid URL- Not An Image";
//        } catch (IOException | TesseractException e) {
//            throw new RuntimeException(e);
//        }
//    }


    private static String convertImageToText(String imageUrl) {
        String convertedText = "Fail To Convert The Image";
        try {
            URL imageURL = new URL(imageUrl);
            URLConnection conn = imageURL.openConnection();
            String contentType = conn.getContentType(); // For example: image/png OR image/gif OR image/jpeg
            String[] imageTypes = contentType.split(Pattern.quote("/")); // e.g: [image, png]
            if (imageTypes[0].equals("image")) { // The url is valid!
                String imageDownloadedName = downloadAndSaveImage(imageURL, imageTypes[1]); // Second argument is image type (jpg/png and so on..)
                if (imageDownloadedName.equals("Fail to download Image"))
                    return "Fail to download Image";
                else {
                    Ocr.setUp();
                    Ocr ocr = new Ocr();
                    ocr.startEngine("eng", Ocr.SPEED_FASTEST); // English
                    convertedText = ocr.recognize(new File[]{new File(imageDownloadedName)}, Ocr.RECOGNIZE_TYPE_ALL, Ocr.OUTPUT_FORMAT_PLAINTEXT);
                    ocr.stopEngine();
                    return convertedText;
                }
            } else
                return "Invalid URL- Not An Image";
        } catch (Exception | Error e) {
            System.out.println("ERROR!!");
            return convertedText;
        }
    }

    private static String downloadAndSaveImage(URL imageURL, String imageType) {
        try {
            InputStream is = imageURL.openStream();
            String downloadImageName = "imageToConvert" + "." + imageType;
            OutputStream os = new FileOutputStream(downloadImageName);
            byte[] bytes = new byte[2048];
            int length;
            while ((length = is.read(bytes)) != -1)
                os.write(bytes, 0, length);
            is.close();
            os.close();
            return downloadImageName;
        } catch (Exception ignore) {
        }
        return "Fail to download Image";
    }


    private static String createDoneTaskMessage(String localAppId, String originUrl, String output, String index) {
        StringBuilder message = new StringBuilder();
        message.append(AWS.TaskType.DoneTask.ordinal());
        message.append(DELIMITER_1);
        message.append(localAppId);
        message.append(DELIMITER_1);
        message.append(originUrl);
        message.append(DELIMITER_1);
        message.append(output);
        message.append(DELIMITER_1);
        message.append(index);
        System.out.println("[DEBUG] Message to manger from worker: " + message);
        return message.toString();
    }
}

