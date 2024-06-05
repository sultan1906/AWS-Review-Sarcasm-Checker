import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Manager {
    final static AWSManager awsManager = AWSManager.getInstance();
    public static void main(String[] args) {
        String globalSQSURL = awsManager.CheckGlobalSQS();

        String messageReceiverSQSURL = awsManager.CheckMassagesReceiverSOS();

        String managerToWorkerSQSUrl = awsManager.CheckAndCreateManagerToWorkerSQS();

        awsManager.createOutputFilesBucketBucketIfNotExists();

        ListeningThread listeningThread = new ListeningThread(globalSQSURL);
        Thread listening = new Thread(listeningThread);

        ExecutorService executorAction = Executors.newFixedThreadPool(4);
        for (int i = 0; i<5; i++){
            ActionThread actionThread = new ActionThread(globalSQSURL,managerToWorkerSQSUrl);
            executorAction.submit(actionThread);
        }

        ExecutorService executorReceiver = Executors.newFixedThreadPool(4);
        for (int i = 0; i<5; i++){
            ReceivingThread receivingThread = new ReceivingThread(messageReceiverSQSURL,managerToWorkerSQSUrl);
            executorReceiver.submit(receivingThread);
        }

        listening.start();

        try {
            listening.join();
            executorAction.shutdown();
            executorAction.awaitTermination(Long.MAX_VALUE, java.util.concurrent.TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            executorReceiver.shutdown();
            executorReceiver.awaitTermination(Long.MAX_VALUE, java.util.concurrent.TimeUnit.NANOSECONDS);
            awsManager.shutdownInstance();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
