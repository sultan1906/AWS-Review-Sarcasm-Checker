import software.amazon.awssdk.services.sqs.model.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReceivingThread implements Runnable {
    final private AWSManager awsManager;
    final private String MassagesReceiverSQSURL;
    final private String managerToWorkerSQSURL;

    public ReceivingThread(String MassagesReceiverSQSURL, String managerToWorkerSQSURL) {
        awsManager = AWSManager.getInstance();
        this.MassagesReceiverSQSURL = MassagesReceiverSQSURL;
        this.managerToWorkerSQSURL = managerToWorkerSQSURL;
    }

    @Override
    public void run() {
        while (true) {
            List<Message> requests = MassagesReceiverSQSMessages();
            if (requests.isEmpty()) {
                sleep(2000);
            } else {
                AtomicBoolean finishProcessRequests = new AtomicBoolean(false);
                makeMessagesVisibilityDynamic(requests, finishProcessRequests);
                processMessages(requests, finishProcessRequests);
                deleteProcessedMessages(requests);
            }
            if(handleTerminationIfNeeded()) return;
        }
    }

    private List<Message> MassagesReceiverSQSMessages() {
        try {
            synchronized (awsManager.receivingMessagesFromWorkers) {
                ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                        .queueUrl(MassagesReceiverSQSURL)
                        .visibilityTimeout(20)
                        .messageAttributeNames("All")
                        .maxNumberOfMessages(10)
                        .build();
                return awsManager.sqs.receiveMessage(receiveRequest).messages();
            }
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }


    /**
     * Processes the messages received from the SQS queue.
     *
     * @param requests               List of messages received from the SQS queue.
     * @param finishProcessRequests AtomicBoolean indicating whether the process should be finished.
     */
    private void processMessages(List<Message> requests, AtomicBoolean finishProcessRequests) {
        for (Message message : requests) {
            Map<String, MessageAttributeValue> attributes = message.messageAttributes();
            String localSQSUrl = attributes.get("localSQSUrl").stringValue();
            String answer = message.body();
            String fileName = getFileName(localSQSUrl);
            if (fileName == null) {
                finishProcessRequests.set(true);
                break;
            }
            writeFile(fileName, answer, finishProcessRequests);
            processReviews(localSQSUrl, fileName, finishProcessRequests);
        }
        finishProcessRequests.set(true);
    }

    private void deleteProcessedMessages(List<Message> requests) {
        for (Message message : requests) {
            awsManager.deleteMessageFromReceiverSQS(message, MassagesReceiverSQSURL);
        }
    }

    /**
     * Writes content to a file.
     *
     * @param fileName               The name of the file.
     * @param content                The content to be written to the file.
     */
    private static void writeToFile(String fileName, String content) throws IOException {
        Path filePath = Paths.get(fileName);
        Files.write(filePath, content.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }


    private void makeMessagesVisibilityDynamic(List<Message> messages, AtomicBoolean finishedWork) {
        Thread timerThread = new Thread(() -> {
            Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    for (Message message : messages) {
                        String receiptHandle = message.receiptHandle();
                        if (!finishedWork.get()) {
                            changeMessageVisibilityRequest(receiptHandle);
                        } else {
                            timer.cancel();
                            break;
                        }
                    }
                }
            }, 100, 15 * 1000);
        });
        timerThread.start();
    }

    public void changeMessageVisibilityRequest(String receiptHandle) {
        awsManager.sqs.changeMessageVisibility(ChangeMessageVisibilityRequest.builder()
                .queueUrl(MassagesReceiverSQSURL)
                .visibilityTimeout(20)
                .receiptHandle(receiptHandle)
                .build());
    }

    private void terminateWorkers(int numOfWorkers) {
        for (int i = 0; i < numOfWorkers; i++) // Send Terminate message to all activate workers
            sendTerminateMessageToWorkers();
    }

    private void sendTerminateMessageToWorkers() {
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(managerToWorkerSQSURL)
                .messageBody("terminate!")
                .messageAttributes(
                        Map.of(
                                "Terminate", MessageAttributeValue.builder().dataType("String").stringValue("Terminate").build()
                        )
                )
                .build();
        awsManager.sqs.sendMessage(send_msg_request);
    }

    private String getFileName(String localSQSUrl) {
        try {
            synchronized (awsManager.createFilesLock) {
                if (awsManager.MapOfNameFiles.get(localSQSUrl) == null) {
                    String fileName = "Answer" + awsManager.filesCounter.get() + ".txt";
                    awsManager.MapOfNameFiles.put(localSQSUrl, fileName);
                    awsManager.filesCounter.getAndIncrement();
                    return fileName;
                } else {
                    return awsManager.MapOfNameFiles.get(localSQSUrl);
                }
            }
        } catch (Exception e) {
            return null; // Or any default value you prefer
        }
    }

    /**
     * Writes content to a file.
     *
     * @param fileName               The name of the file.
     * @param content                The content to be written to the file.
     * @param finishProcessRequests  AtomicBoolean indicating whether the process should be finished.
     */
    private void writeFile(String fileName, String content, AtomicBoolean finishProcessRequests) {
        try {
            synchronized (awsManager.createFilesLock) {
                writeToFile(fileName, content);
            }
        } catch (IOException e) {
            finishProcessRequests.set(true);
        }
    }

    /**
     * Process reviews received from a specific SQS URL.
     * Decrements the count of reviews for the given URL in the MapOfReviews map.
     * If the count becomes zero after decrementing, removes the entry from the map,
     * uploads the corresponding file to an S3 bucket, and sends a message to another SQS queue with the file name.
     *
     * @param localSQSUrl            The local SQS URL for which reviews are being processed.
     * @param fileName               The file name associated with the reviews.
     * @param finishProcessRequests  AtomicBoolean indicating whether the process should be finished.
     */
    private void processReviews(String localSQSUrl, String fileName, AtomicBoolean finishProcessRequests) {
        try {
            synchronized (awsManager.NumOfReviewsLock) {
                awsManager.MapOfReviews.replace(localSQSUrl, awsManager.MapOfReviews.get(localSQSUrl) - 1);
                int numOfReviews = awsManager.MapOfReviews.get(localSQSUrl);
                if (numOfReviews == 0) {
                    awsManager.MapOfReviews.remove(localSQSUrl);
                    awsManager.uploadFileToS3(fileName);
                    awsManager.SendToSQS(localSQSUrl, fileName);
                }
            }
        } catch (Exception e) {
            finishProcessRequests.set(true);
        }
    }

    /**
     * Handles termination of workers if necessary based on certain conditions.
     */
    private boolean handleTerminationIfNeeded() {
        try {
            synchronized (awsManager.NumOfReviewsLock) {
                if (awsManager.MapOfReviews.isEmpty() && awsManager.terminate.get()) {
                    if (awsManager.WorkersCounter.get() == 0) {
                        int numOfRunningWorkers = awsManager.checkHowManyWorkersRunning();
                        terminateWorkers(numOfRunningWorkers);
                        awsManager.WorkersCounter.set(1);
                    }
                    sleep(5000);
                    return true;
                }
            }
        } catch (Exception e) {
            System.out.println("[ERROR] " + e.getMessage());
        }
        return false;
    }

    private static void sleep(int milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            System.out.println("[ERROR] " + e.getMessage());
        }
    }
}


