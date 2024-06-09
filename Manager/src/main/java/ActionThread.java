import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.utils.Pair;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import static java.lang.Integer.parseInt;

public class ActionThread implements Runnable {
    final private AWSManager awsManager;
    final private String managerToWorkerSQSURL;
    final private String globalSQSURL;

    public ActionThread(String globalSQSURL, String managerToWorkerSQSURL) {
        this.globalSQSURL = globalSQSURL;
        this.managerToWorkerSQSURL = managerToWorkerSQSURL;
        awsManager = AWSManager.getInstance();
    }

    @Override
    public void run() {
        while (!awsManager.terminate.get()) {
            if (awsManager.QueueOfRequests.isEmpty()) {
                sleep(2000);
            } else {
                Message message = awsManager.QueueOfRequests.poll();
                handleRequestMessage(message);
            }
        }
    }

    /**
     * Handles the processing of a single SQS request message.
     *
     * @param message The SQS message to be processed.
     */
    private void handleRequestMessage(Message message) {
        String sqsLocalUrl = message.body();
        Map<String, MessageAttributeValue> attributes = message.messageAttributes();

        String bucketName = getAttribute(attributes, "Bucket");
        String fileName = getAttribute(attributes, "File");
        String n = getAttribute(attributes, "n");
        String terminate = getAttribute(attributes, "Terminate");

        awsManager.MapOfReviews.put(sqsLocalUrl, 0);
        int sumOfReviews = processRequest(bucketName, fileName, sqsLocalUrl);

        manageWorkerCreation(sumOfReviews, n);

        if (terminate != null && !terminate.isEmpty()) {
            awsManager.terminate.set(true);
        }

        awsManager.deleteMessageFromGlobalSqs(globalSQSURL, message);
    }

    private static void sleep(int milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            System.out.println("[ERROR] " + e.getMessage());
        }
    }

    /**
     * Retrieves the value of a specified attribute from a message's attributes.
     *
     * @param attributes The map of message attributes.
     * @param key        The key of the attribute to retrieve.
     * @return The string value of the attribute, or null if the attribute is not found.
     */
    private String getAttribute(Map<String, MessageAttributeValue> attributes, String key) {
        MessageAttributeValue attribute = attributes.get(key);
        return (attribute != null) ? attribute.stringValue() : null;
    }

    /**
     * Manages the creation of worker instances based on the number of reviews and the provided parameter.
     *
     * @param sumOfReviews The total number of reviews processed.
     * @param n            The parameter to determine the number of workers to create.
     */
    private void manageWorkerCreation(int sumOfReviews, String n) {
        try {
            synchronized (awsManager.updateWorkersLock) {
                int numOfWorkers = awsManager.checkHowManyWorkersRunning();
                if (numOfWorkers < 8) {
                    int num = sumOfReviews / parseInt(n);
                    if (num <= 8 && num > numOfWorkers) {
                        awsManager.createWorkers(num - numOfWorkers);
                    } else if (num > 8) {
                        awsManager.createWorkers(8 - numOfWorkers);
                    }
                }
            }
        } catch (Exception e) {
            // Handle the exception if needed
        }
    }

    /**
     * Processes the request by reading a file from the S3 bucket and processing its content.
     *
     * @param bucketName The name of the S3 bucket.
     * @param fileName   The name of the file in the S3 bucket.
     * @param sqsLocalUrl The local SQS URL for sending processed data.
     * @return The total number of reviews processed.
     */
    private int processRequest(String bucketName, String fileName, String sqsLocalUrl) {
        int sumOfReviews = 0;
        try {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(fileName)
                    .build();

            ResponseInputStream<GetObjectResponse> s3Object = awsManager.s3.getObject(getObjectRequest);

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    sumOfReviews += processReview(line, sqsLocalUrl);
                }

            } catch (IOException e) {
                System.err.println("[ERROR] " + e.getMessage());
            }
        } catch (Exception e) {
            System.err.println("[ERROR] " + e.getMessage());
        }
        return sumOfReviews;
    }

    /**
     * Processes a single review from a JSON line.
     *
     * @param jsonLine   The JSON line containing review data.
     * @param sqsLocalUrl The local SQS URL for sending processed data.
     * @return The number of reviews processed from the JSON line.
     */
    private int processReview(String jsonLine, String sqsLocalUrl) {
        int counter = 0;
        JsonParser parser = new JsonParser();
        JsonObject jsonObject = parser.parse(jsonLine).getAsJsonObject();
        JsonArray reviews = jsonObject.getAsJsonArray("reviews");
        for (int i = 0; i < reviews.size(); i++) {
            JsonObject review = reviews.get(i).getAsJsonObject();
            String reviewText = review.getAsJsonPrimitive("text").getAsString();
            String reviewUrl = review.get("link").getAsString();
            String rating = review.get("rating").getAsString();
            Pair<String, String> urlReviewPair = Pair.of(reviewUrl, reviewText);
            awsManager.SendToManagerToWorkerSQS(sqsLocalUrl, managerToWorkerSQSURL, urlReviewPair, rating);
            counter++;
        }
        try {
            synchronized (awsManager.NumOfReviewsLock) {
                awsManager.MapOfReviews.replace(sqsLocalUrl, awsManager.MapOfReviews.get(sqsLocalUrl) + counter);
            }
        } catch(Exception e) {
            System.err.println("[ERROR] " + e.getMessage());
        }
        return counter;
    }
}
