
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class LocalAppData {

    private final Map<String, String> tasksStatus = new ConcurrentHashMap<>();
    private final AtomicInteger remainingTasks;
    private final int numberOfTasksPerWorker;

    public LocalAppData(int remainingTasks, int workerCount) {
        this.remainingTasks = new AtomicInteger(remainingTasks);
        this.numberOfTasksPerWorker = workerCount;
    }

    public Map<String, String> getTasksStatus() {
        return tasksStatus;
    }


    public int atomicDecrementTasksCounter() {
        return this.remainingTasks.decrementAndGet();
    }

    public int getNumberOfTasksPerWorker() {
        return numberOfTasksPerWorker;
    }


    public void putIfAbsent(String taskKey, String outputLine) {
        tasksStatus.putIfAbsent(taskKey, outputLine);
    }
}
