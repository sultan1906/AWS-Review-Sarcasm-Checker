import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
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
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    System.out.println("[ERROR] " + e.getMessage());
                }
            } else {
                AtomicBoolean finishProcessRequests = new AtomicBoolean(false);
                makeMessagesVisibilityDynamic(requests, finishProcessRequests);
                for (Message message : requests) {
                    Map<String, MessageAttributeValue> attributes = message.messageAttributes();
                    String localSQSUrl = attributes.get("localSQSUrl").stringValue();
                    String answer = message.body();
                    String fileName;
                    try {
                        synchronized (awsManager.createFilesLock) {
                            if (awsManager.MapOfNameFiles.get(localSQSUrl) == null) {
                                fileName = "Answer" + awsManager.filesCounter.get() + ".txt";
                                awsManager.MapOfNameFiles.put(localSQSUrl, fileName);
                                awsManager.filesCounter.getAndIncrement();
                            } else {
                                fileName = awsManager.MapOfNameFiles.get(localSQSUrl);
                            }
                        }
                    } catch (Exception e) {
                        finishProcessRequests.set(true);
                        break;
                    }

                    try {
                        synchronized (awsManager.createFilesLock) {
                            writeToFile(fileName, answer);
                        }
                    } catch (IOException e) {
                        finishProcessRequests.set(true);
                        break;
                    }
                    try {
                        synchronized (awsManager.NumOfReviewsLock) {
                            awsManager.MapOfReviews.replace(localSQSUrl, awsManager.MapOfReviews.get(localSQSUrl) - 1);
                            int numOfReviews = awsManager.MapOfReviews.get(localSQSUrl);
                            if (numOfReviews == 0) {
                                awsManager.MapOfReviews.remove(localSQSUrl);
                                uploadFileToS3(fileName);
                                SendToSQS(localSQSUrl, fileName);
                            }
                        }
                    } catch (Exception e) {
                        finishProcessRequests.set(true);
                        break;
                    }
                }
                finishProcessRequests.set(true);
                for (Message m : requests) {
                    deleteMessageFromReceiverSQS(m);
                }
            }
            try {
                synchronized (awsManager.NumOfReviewsLock) {
                    if (awsManager.MapOfReviews.isEmpty() && awsManager.terminate.get()) {
                        if (awsManager.WorkersCounter.get() == 0) {
                            int numOfRunningWorkers = checkHowManyWorkersRunning();
                            terminateWorkers(numOfRunningWorkers);
                            awsManager.WorkersCounter.set(1);
                        }
                        Thread.sleep(5000);
                        return;
                    }
                }
            } catch (Exception e) {
                System.out.println("[ERROR] " + e.getMessage());
            }
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

    private static void writeToFile(String fileName, String content) throws IOException {
        Path filePath = Paths.get(fileName);
        Files.write(filePath, content.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    private void uploadFileToS3(String answerFileName) {
        Path filePath = Paths.get(answerFileName);
        String key = filePath.getFileName().toString();

        awsManager.s3.putObject(PutObjectRequest.builder()
                .bucket(awsManager.bucketName)
                .key(key)
                .build(), RequestBody.fromFile(filePath));
    }

    private void SendToSQS(String sqsUrl, String fileName) {
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(sqsUrl)
                .messageBody(fileName)
                .build();
        awsManager.sqs.sendMessage(send_msg_request);
    }

    private void deleteMessageFromReceiverSQS(Message message) {
        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(MassagesReceiverSQSURL)
                .receiptHandle(message.receiptHandle())
                .build();
        awsManager.sqs.deleteMessage(deleteRequest);
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

    private int checkHowManyWorkersRunning() {
        String nextToken = null;
        int counter = 0;

        do {
            DescribeInstancesRequest request = DescribeInstancesRequest
                    .builder().nextToken(nextToken).build();
            DescribeInstancesResponse response = awsManager.ec2.describeInstances(request);

            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    if (instance.state().name().toString().equals("running")) {
                        for (software.amazon.awssdk.services.ec2.model.Tag tag : instance.tags()) {
                            if (tag.key().equals("Name") && tag.value().equals("Worker")) {
                                counter++;
                            }
                        }
                    }
                }
            }
            nextToken = response.nextToken();

        } while (nextToken != null);

        return counter;
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
}


