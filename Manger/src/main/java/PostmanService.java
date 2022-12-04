

import software.amazon.awssdk.services.sqs.model.Message;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


public class PostmanService {

    // Singleton
    private final AWS aws = AWS.getInstance();

    // Constants
    public static final int MAX_WORKERS_INSTANCES = 17;
    public static final int NUM_THREADS_LOCAL_POOL = 3;
    public static final int NUM_THREADS_WORKER_POOL = 3;
    public static final int NUM_THREADS_PING_POOL = 3;
    public final static String DELIMITER_1 = "delimiter_1";
    public final static String DELIMITER_2 = "delimiter_2";

    // Data Structures
    private final ConcurrentLinkedQueue<Message> localTasksQueue = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<Message> doneTasksQueue = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<Message> pingTaskQueue = new ConcurrentLinkedQueue<>();

    private final ConcurrentHashMap<String, LocalAppData> localApps = new ConcurrentHashMap<>(); // <Local ID, Local Data>

    // Atomics
    private final AtomicBoolean shouldTerminate = new AtomicBoolean(false);
    private final AtomicInteger numberOfRemainingTasksTotal = new AtomicInteger(0);
    private final AtomicInteger numberOfTasksPerWorkerForAllLocalApps = new AtomicInteger(0);
    private final AtomicInteger numberOfActivateWorker = new AtomicInteger(0);


    // Locks
    final private Object localTaskLock = new Object();
    final private Object doneTaskLock = new Object();
    final private Object pingTaskLock = new Object();
    final private Object updateWorkersLock = new Object();

    // Constructor
    private static final PostmanService instance = new PostmanService();

    // Private constructor for Singleton
    private PostmanService() {
        System.out.println("[DEBUG] Postman starts working and initialized all Thread pools");
    }

    public static PostmanService getInstance() {
        return instance;
    }


    public void run() {
        createThreadPools();
        while (true) {
            String managerQueueUrl = aws.getQueueUrl(aws.managerQueueName);
            Message message = aws.getNewMessage(managerQueueUrl);
            if (message != null) {
                System.out.println("[DEBUG] Postman found a messages and deleting her from the sqs queue");
                aws.deleteMessageFromQueue(managerQueueUrl, message);
                String[] msgParts = message.body().split(DELIMITER_1); // [Type, Delimiter, Bucket Name, Delimiter, key Name, Delimiter, Task Per Worker]
                System.out.println("[DEBUG] Message do deliver: " + Arrays.toString(msgParts));
                int messageType = Integer.parseInt(msgParts[0]);
                if (itsNewTaskMessage(messageType))
                    handleNewTaskMessage(message);
                else if (itsDoneTaskMessage(messageType))
                    handleDoneTaskMessage(message);
                else if (itsPingMessage(messageType)) { // TODO: check about FIFO imp for ping issue
                    handlePingTaskMessage(message);
                } else { // Terminate message
                    shouldTerminate.set(true);
                    terminateIfTasksAreDone();
                }
            }
        }
    }

    private void createThreadPools() {
        ExecutorService localTaskHandler = Executors.newFixedThreadPool(NUM_THREADS_LOCAL_POOL);
        for (int i = 0; i < NUM_THREADS_LOCAL_POOL; i++)
            localTaskHandler.execute(new LocalTaskHandler());

        ExecutorService workerTaskThreadPool = Executors.newFixedThreadPool(NUM_THREADS_WORKER_POOL);
        for (int i = 0; i < NUM_THREADS_WORKER_POOL; i++)
            workerTaskThreadPool.execute(new WorkerTaskHandler());

        ExecutorService pingTaskThreadPool = Executors.newFixedThreadPool(NUM_THREADS_PING_POOL);
        for (int i = 0; i < NUM_THREADS_PING_POOL; i++)
            pingTaskThreadPool.execute(new PingTaskHandler());
    }

    private void handleNewTaskMessage(Message message) {
        synchronized (localTaskLock) {
            if (!shouldTerminate.get()) {
                localTasksQueue.add(message);
                System.out.println("[DEBUG] Postman adding a massage to localTasksQueue and Waking up Local handler pool");
                localTaskLock.notify();
            }
        }
    }

    private void handleDoneTaskMessage(Message message) {
        synchronized (doneTaskLock) {
            doneTasksQueue.add(message);
            System.out.println("[DEBUG] Postman adding a massage to doneTasksQueue and Waking up Worker handler pool");
            doneTaskLock.notify();
        }
    }

    private void handlePingTaskMessage(Message message) {
        synchronized (pingTaskLock) {
            pingTaskQueue.add(message);
            System.out.println("[DEBUG] Postman adding a massage to pingTaskQueue and Waking up Ping handler pool");
            pingTaskLock.notify();
        }
    }

    public void updateNumberOfWorkers() {
        synchronized (updateWorkersLock) {
            int numOfActivateWorker = numberOfActivateWorker.get();
            double numberOfRemainingTasksTotal = getNumberOfRemainingTasksTotal(); // Remaining Tasks for *ALL* locals ups
            double numberOfTasksPerWorkerForAllLocalApp = getNumberOfTasksPerWorkerForAllLocalApps(); // Number of tasks per worker Tasks for *ALL* locals ups
            System.out.println("[DEBUG] numberOfActivateWorker: " + numOfActivateWorker
                    + " numberOfRemainingTasksTotal: " + (int) numberOfRemainingTasksTotal + " numberOfTasksPerWorkerForAllLocalApp: " + (int) numberOfTasksPerWorkerForAllLocalApp);
            int numberOfWorkersNeeds = Math.min((int) Math.ceil(numberOfRemainingTasksTotal / numberOfTasksPerWorkerForAllLocalApp), MAX_WORKERS_INSTANCES);
            System.out.println("[DEBUG] Expected number of workers Needs: " + numberOfWorkersNeeds
                    + " Current number of workers: " + numOfActivateWorker);
            if (numberOfWorkersNeeds - numOfActivateWorker != 0) { // If its 0 do nothing
                if (numberOfWorkersNeeds - numOfActivateWorker > 0) { // Add Workers
                    if ((numOfActivateWorker + (numberOfWorkersNeeds - numOfActivateWorker)) > MAX_WORKERS_INSTANCES)  // Preventing exceeding max instance that user allowed
                        createNewWorkers(MAX_WORKERS_INSTANCES - numOfActivateWorker); // Add maximum workers that allowed
                    else
                        createNewWorkers(numberOfWorkersNeeds - numOfActivateWorker);
                } else  // Remove workers
                    removeWorkers(numOfActivateWorker - numberOfWorkersNeeds, numOfActivateWorker);
            }
        }
    }

    private void createNewWorkers(int numberOfWorkersToAdd) {
        synchronized (updateWorkersLock) {
            String workerScript = "#! /bin/bash\n" +
                    "echo Worker jar running\n" +
                    "mkdir WorkerFiles\n" +
                    "aws s3 cp " + "s3://" + aws.bucketName + "/" + aws.workerJarKey + " ./WorkerFiles/" + aws.workerJarName + "\n" +
                    "java -jar /WorkerFiles/" + aws.workerJarName + "\n";
            System.out.println("[DEBUG] Generates " + (numberOfWorkersToAdd) + " new workers.");
            // Generate numberOfWorkersToAdd workers
            try {
                for (int i = 0; i < numberOfWorkersToAdd; i++)
                    aws.createInstance(workerScript, aws.workerTag, 1);
                numberOfActivateWorker.addAndGet(numberOfWorkersToAdd);
            } catch (Exception e) {
                System.out.println("[ERROR] Error while manger trying create workers: " + e.getMessage());
            }
        }
    }

    private void removeWorkers(int numberOfWorkersToRemove, int numOfActivateWorker) {
        synchronized (updateWorkersLock) {
            System.out.println("[DEBUG] Removing " +
                    numberOfWorkersToRemove + " workers. Remaining workers: " + (numOfActivateWorker - numberOfWorkersToRemove));
            String workerQueueUrl = aws.getQueueUrl(aws.workerQueueName);
            for (int i = 0; i < numberOfWorkersToRemove; i++)
                aws.sendMessage(workerQueueUrl,
                        createTerminateWorkerMsg());
            numberOfActivateWorker.addAndGet(numberOfWorkersToRemove * -1);
        }
    }

    private String createTerminateWorkerMsg() {
        return String.valueOf(AWS.TaskType.Terminate.ordinal());
    }

    public ConcurrentHashMap<String, LocalAppData> getLocalApps() {
        return localApps;
    }

    public Message getLocalTaskMessage() {
        synchronized (localTaskLock) {
            while (!localTasksQueue.isEmpty())
                return localTasksQueue.poll();
            try {
                System.out.println("[DEBUG] Local Handler pool didnt find a message and going to sleep");
                localTaskLock.wait();
            } catch (InterruptedException ignored) {

            }
        }
        return null;
    }

    public Message getPingTaskMessage() {
        synchronized (pingTaskLock) {
            while (!pingTaskQueue.isEmpty())
                return pingTaskQueue.poll();
            try {
                System.out.println("[DEBUG] Ping Handler pool didnt find a message and going to sleep");
                pingTaskLock.wait();
            } catch (InterruptedException ignored) {
            }
        }
        return null;
    }

    public Message getWorkerTaskMessage() {
        synchronized (doneTaskLock) {
            while (!doneTasksQueue.isEmpty())
                return doneTasksQueue.poll();

            try {
                System.out.println("[DEBUG] Worker Handler pool didnt find a message and going to sleep");
                doneTaskLock.wait();
            } catch (InterruptedException ignored) {
            }
        }
        return null;
    }


    private boolean itsPingMessage(int messageType) {
        return messageType == AWS.TaskType.Ping.ordinal();
    }

    private boolean itsDoneTaskMessage(int messageType) {
        return messageType == AWS.TaskType.DoneTask.ordinal();
    }

    private boolean itsNewTaskMessage(int messageType) {
        return messageType == AWS.TaskType.NewTask.ordinal();
    }

    public int addNumberOfRemainingTasksTotal(int tasksOfSpecificLocal) {
        synchronized (updateWorkersLock) {
            return numberOfRemainingTasksTotal.addAndGet(tasksOfSpecificLocal);
        }
    }

    public void addNumberOfTasksPerWorkerForAllLocalApps(int numberOfTasksPerWorker) {
        synchronized (updateWorkersLock) {
            numberOfTasksPerWorkerForAllLocalApps.addAndGet(numberOfTasksPerWorker);
        }
    }

    public int decrementRemainingTasksTotal() {
        synchronized (updateWorkersLock) {
            return numberOfRemainingTasksTotal.decrementAndGet();
        }
    }

    public void decrementTasksPerWorkerForAllLocalApps(int numberOfTasksPerWorker) {
        synchronized (updateWorkersLock) {
            numberOfTasksPerWorkerForAllLocalApps.addAndGet(numberOfTasksPerWorker);
        }
    }

    private int getNumberOfRemainingTasksTotal() {
        synchronized (updateWorkersLock) {
            return numberOfRemainingTasksTotal.get();
        }
    }

    public int getNumberOfTasksPerWorkerForAllLocalApps() {
        synchronized (updateWorkersLock) {
            return numberOfTasksPerWorkerForAllLocalApps.get();
        }
    }

    public Object getUpdateWorkersLock() {
        return updateWorkersLock;
    }

    public void terminateIfTasksAreDone() {
        if (localApps.isEmpty()) {
            System.out.println("[DEBUG] Terminating manager and all activate workers including SQS queues and deleting bucket from s3.");
            try { // The try/catch Added because sometimes Worker Q deleted before trying to do next line
                String workerQueueUrl = aws.getQueueUrl(aws.workerQueueName);
                int numberOfActivateWorker = aws.getNumOfInstances(aws.workerTag);
                for (int i = 0; i < numberOfActivateWorker; i++) // Send Terminate message to all activate workers
                    aws.sendMessage(workerQueueUrl, createTerminateWorkerMsg());
                aws.deleteQueue(aws.getQueueUrl(aws.managerQueueName));
                Thread.sleep(1000 * 120); // Sleeping for 120 second to let all workers time to find termination message before terminate their sqs queue
                numberOfActivateWorker = aws.getNumOfInstances(aws.workerTag);
                if (numberOfActivateWorker == 0) { // No more workers so he's deleting the workers sqs Q and terminate himself
                    System.out.println("[DEBUG] Deleting Worker workers sqs queue");
                    aws.deleteQueue(workerQueueUrl);
                }
                aws.deleteBucket(aws.bucketName);
                aws.shutdownInstance(); // Terminates the manager (if localApps is empty all threads are not working on tasks)
//                System.exit(0); // For local debugging

            } catch (Exception | Error e) {
                System.out.println("[ERROR] " + e.getMessage());
            }
        }
    }
}
