import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Integer.parseInt;

public class Worker {
    static SentimentAnalysisHandler sentimentAnalysisHandler = new SentimentAnalysisHandler();
    static NamedEntityRecognitionHandler namedEntityRecognitionHandler = new NamedEntityRecognitionHandler();
    final static AWSWorker awsWorker = AWSWorker.getInstance();

    public static void main(String[] args) {
        String managerToWorker = awsWorker.CheckSQS(awsWorker.managerToWorkerSQS);

        String MassagesReceiver = awsWorker.CheckSQS(awsWorker.MassagesReceiverSOS);

        while (true) {
            List<Message> messages = awsWorker.GetFromManager(managerToWorker);
            if (messages.isEmpty()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    System.out.println("[ERROR] " + e.getMessage());
                }
            } else {
                for (Message message : messages) {
                    AtomicBoolean finishedWork = new AtomicBoolean(false); // for visibility extend
                    makeMessageVisibilityDynamic(message, MassagesReceiver, finishedWork);

                    String link;
                    String rating;
                    String sqsLocalUrl;
                    Map<String, MessageAttributeValue> attributes = message.messageAttributes();
                    MessageAttributeValue linkAttribute = attributes.get("Link");
                    MessageAttributeValue ratingAttribute = attributes.get("Rating");
                    MessageAttributeValue sqsLocalUrlAttribute = attributes.get("SQSLocalUrl");
                    link = (linkAttribute != null) ? linkAttribute.stringValue() : null;
                    rating = (ratingAttribute != null) ? ratingAttribute.stringValue() : null;
                    sqsLocalUrl = (sqsLocalUrlAttribute != null) ? sqsLocalUrlAttribute.stringValue() : null;

                    if (message.body().equals("terminate!")){
                        awsWorker.deleteMessageFromManagerToWorkerSQS(managerToWorker, message);
                        finishedWork.set(true);
                        try {
                            Thread.sleep(10000);
                        } catch (InterruptedException e) {
                            System.out.println("[ERROR] " + e.getMessage());
                        }
                        awsWorker.shutdownInstance();
                        return;
                    }

                    int sentiment = processSentimentReview(message.body());
                    String entities = processEntitiesReview(message.body());
                    String sarcasm = processSarcasmReview(parseInt(rating), sentiment);

                    String response = buildString(sentiment, link, entities, sarcasm).toString();

                    awsWorker.SendToManagerSQS(MassagesReceiver, sqsLocalUrl, response);

                    awsWorker.deleteMessageFromManagerToWorkerSQS(managerToWorker, message);

                    finishedWork.set(true);
                }
            }
        }
    }

    private static int processSentimentReview(String review) {
        return sentimentAnalysisHandler.findSentiment(review);
    }

    private static String processEntitiesReview(String review) {
        return namedEntityRecognitionHandler.printEntities(review);
    }

    private static String processSarcasmReview(int rating, int sentiment) {
        return rating != sentiment ?  "Sarcasm" : "No Sarcasm";
    }

    /**
     * Dynamically adjusts the visibility timeout of a message in an SQS queue.
     *
     * @param message       The message whose visibility timeout needs adjustment.
     * @param workerQueueUrl The URL of the SQS queue where the message resides.
     * @param finishedWork  A flag indicating whether the processing of the message has finished.
     */
    private static void makeMessageVisibilityDynamic(Message message, String workerQueueUrl, AtomicBoolean finishedWork) {
        String receiptHandle = message.receiptHandle();
        Thread timerThread = new Thread(() -> {
            Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    if (!finishedWork.get())
                        awsWorker.changeMessageVisibilityRequest(workerQueueUrl, receiptHandle);
                    else {
                        timer.cancel();
                    }
                }
            }, 100, 10 * 1000);
        });
        timerThread.start();
    }

    public static StringBuilder buildString(int sentiment, String link, String entities, String sarcasm){
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("Sentiment: ").append(sentiment).append("\n");
        stringBuilder.append("Link: ").append(link).append("\n");
        stringBuilder.append("Entities: ").append(entities).append("\n");
        stringBuilder.append("Sarcasm: ").append(sarcasm);
        stringBuilder.append("\n");
        stringBuilder.append("\n");

        return stringBuilder;
    }
}



