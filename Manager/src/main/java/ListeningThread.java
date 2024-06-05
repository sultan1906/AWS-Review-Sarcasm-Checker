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
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                }
            }
            else {
                for (Message curr : requests) {
                    awsManager.QueueOfRequests.offer(curr);
                }
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }
    }
}
