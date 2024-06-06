import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;

public class ListeningThread implements Runnable{
    final private AWSManager awsManager;
    final private String globalSQSURL;
    public ListeningThread (String globalSQSURL){
        awsManager = AWSManager.getInstance();
        this.globalSQSURL = globalSQSURL;
    }
    @Override
    public void run() {
        while (!awsManager.terminate.get()){
            List<Message> requests = awsManager.GlobalSQSMessages(globalSQSURL);
            if(requests.isEmpty()) {
                sleep(2000);
            }
            else {
                for (Message curr : requests) {
                    awsManager.QueueOfRequests.offer(curr);
                }
            }
            sleep(100);
        }
    }

    private static void sleep(int milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            System.out.println("[ERROR] " + e.getMessage());
        }
    }
}
