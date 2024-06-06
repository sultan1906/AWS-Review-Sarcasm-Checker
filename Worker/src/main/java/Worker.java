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
        String messagesReceiver = awsWorker.CheckSQS(awsWorker.MassagesReceiverSOS);

        boolean shouldTerminate = false;

        while (!shouldTerminate) {
            List<Message> messages = awsWorker.GetFromManager(managerToWorker);
            if (messages.isEmpty()) {
                sleep(2000);
            } else {
                shouldTerminate = processMessages(messages, managerToWorker, messagesReceiver);
            }
        }
    }

    /**
     * Processes the messages received from the SQS queue.
     *
     * @param messages List of messages received from the SQS queue.
     * @param managerToWorker The SQS URL for the manager to worker communication.
     * @param messagesReceiver The SQS URL for receiving processed messages.
     * @return true if a terminate message is received, false otherwise.
     */
    private static boolean  processMessages(List<Message> messages, String managerToWorker, String messagesReceiver) {
        for (Message message : messages) {
            AtomicBoolean finishedWork = new AtomicBoolean(false); // for visibility extend
            makeMessageVisibilityDynamic(message, messagesReceiver, finishedWork);

            Map<String, MessageAttributeValue> attributes = message.messageAttributes();
            String link = getMessageAttributeValue(attributes, "Link");
            String rating = getMessageAttributeValue(attributes, "Rating");
            String sqsLocalUrl = getMessageAttributeValue(attributes, "SQSLocalUrl");

            if (message.body().equals("terminate!")) {
                handleTerminateMessage(managerToWorker, message, finishedWork);
                return true;
            }

            processMessage(message, link, rating, sqsLocalUrl, messagesReceiver, managerToWorker);

            finishedWork.set(true);
        }
        return false;
    }

    /**
     * Handles the terminate message and shuts down the instance.
     *
     * @param managerToWorker The SQS URL for the manager to worker communication.
     * @param message The terminate message.
     * @param finishedWork The flag indicating whether the work is finished.
     */
    private static void handleTerminateMessage(String managerToWorker, Message message, AtomicBoolean finishedWork) {
        awsWorker.deleteMessageFromManagerToWorkerSQS(managerToWorker, message);
        finishedWork.set(true);
        sleep(10000);
        awsWorker.shutdownInstance();
    }

    /**
     * Processes an individual message by analyzing its content.
     *
     * @param message The message to process.
     * @param link The link associated with the message.
     * @param rating The rating associated with the message.
     * @param sqsLocalUrl The SQS local URL.
     * @param messagesReceiver The SQS URL for receiving processed messages.
     * @param managerToWorker The SQS URL for the manager to worker communication.
     */
    private static void processMessage(Message message, String link, String rating, String sqsLocalUrl, String messagesReceiver, String managerToWorker) {
        int sentiment = processSentimentReview(message.body());
        String entities = processEntitiesReview(message.body());
        String sarcasm = processSarcasmReview(parseInt(rating), sentiment);

        String response = buildString(sentiment, link, entities, sarcasm).toString();
        awsWorker.SendToManagerSQS(messagesReceiver, sqsLocalUrl, response);
        awsWorker.deleteMessageFromManagerToWorkerSQS(managerToWorker, message);
    }

    private static String getMessageAttributeValue(Map<String, MessageAttributeValue> attributes, String key) {
        MessageAttributeValue attribute = attributes.get(key);
        return (attribute != null) ? attribute.stringValue() : null;
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

    private static void sleep(int milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            System.out.println("[ERROR] " + e.getMessage());
        }
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

    /**
     * Builds a response string from the given parameters.
     *
     * @param sentiment The sentiment score.
     * @param link The link associated with the review.
     * @param entities The extracted entities.
     * @param sarcasm The sarcasm status.
     * @return The response string.
     */
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



