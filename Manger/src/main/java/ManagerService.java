
public class ManagerService {
    public static void main(String[] args) {
        System.out.println("[DEBUG] managerService starts");
        PostmanService postmanService = PostmanService.getInstance();
        postmanService.run();
    }
}
