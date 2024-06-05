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
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    System.err.println("[ERROR] " + e.getMessage());
                }
            } else {
                Message message = awsManager.QueueOfRequests.poll();
                String sqsLocalUrl = message.body();
                Map<String, MessageAttributeValue> attributes = message.messageAttributes();
                MessageAttributeValue bucketAttribute = attributes.get("Bucket");
                MessageAttributeValue fileAttribute = attributes.get("File");
                MessageAttributeValue nAttribute = attributes.get("n");
                MessageAttributeValue terminateAttribute = attributes.get("Terminate");

                String bucketName = (bucketAttribute != null) ? bucketAttribute.stringValue() : null;
                String fileName = (fileAttribute != null) ? fileAttribute.stringValue() : null;
                String n = (nAttribute != null) ? nAttribute.stringValue() : null;
                String terminate = (terminateAttribute != null) ? terminateAttribute.stringValue() : null;

                awsManager.MapOfReviews.put(sqsLocalUrl, 0);
                int sumOfReviews = ProcessRequest(bucketName, fileName, sqsLocalUrl);

                try {
                    synchronized (awsManager.updateWorkersLock) {
                        int numOfWorkers = awsManager.checkHowManyWorkersRunning();
                        if (numOfWorkers < 8) {
                            int num = sumOfReviews / parseInt(n);
                            if (num <= 8 && num > numOfWorkers) {
                                awsManager.createWorkers(num - numOfWorkers);
                            }
                            else if (num > 8){
                                awsManager.createWorkers(8-numOfWorkers);
                            }
                        }
                    }
                } catch (Exception e) {
                    continue;
                }
                if (!terminate.isEmpty()){
                    awsManager.terminate.set(true);
                }
                awsManager.deleteMessageFromGlobalSqs(globalSQSURL, message);
            }
        }
    }

    private int ProcessRequest(String bucketName, String fileName, String sqsLocalUrl) {
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
