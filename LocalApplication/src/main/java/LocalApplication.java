import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.*;
import java.util.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class LocalApplication {
    final static AWS aws = AWS.getInstance();
    private static final String QUEUE_NAME = "LocalApplicationSQS" + new Date().getTime();

    public static void main(String[] args) {
        extractArguments(args);

        String globalSQSURL = aws.checkGlobalSQS();

        aws.createBucketIfNotExists(aws.bucketName);

//        List<String> filePaths = List.of("input1.txt", "input2.txt");
        uploadInputFilesToBucket(aws.bucketName, aws.inputFiles);

        String LocalSQSURL = aws.createLocalSQS(QUEUE_NAME);

        aws.createBucketIfNotExists(aws.jarsBucket);
        uploadJarsToBucket();

        aws.createManagerIfNotExists();

        aws.sendToGlobalSQS(globalSQSURL, LocalSQSURL);

        receiveMassagesFromSQS(LocalSQSURL);

        aws.deleteLocalSQS(LocalSQSURL);

        aws.deleteBucketAndObjects(aws.bucketName, true);

        aws.deleteBucketAndObjects(aws.answersBucket, false);
    }

    private static void extractArguments(String[] args) {
        if (args.length < 3) {
            System.out.println("[ERROR] Missing command line arguments.");
            System.exit(1);
        }
        int i = 0;
        while (i < args.length && args[i].startsWith("input")){
            aws.inputFiles.add(args[i]);
            i++;
        }
        //If there is no output file name and n
        if (i > args.length -2){
            System.out.println("Command line arguments are missing");
            System.exit(1);
        }
        aws.outputFile = args[i];
        i++;
        aws.n = args[i];
        i++;
        if (i == args.length -1){
            aws.terminate = args[i];
        }
    }

    private static void uploadInputFilesToBucket(String bucketName, List<String> files) {
        String combinedFileName = "combinedFiles.txt";
        mergeFiles(files, combinedFileName);
        aws.uploadFileToS3(bucketName, combinedFileName);
    }

    private static void mergeFiles(List<String> files, String combinedFileName) {
        try (PrintWriter writer = new PrintWriter(new FileWriter(combinedFileName))) {
            for (String file : files) {
                Path filePath = Paths.get(file);
                List<String> lines = Files.readAllLines(filePath);
                for (String line : lines) {
                    writer.println(line);
                }
            }
        } catch (IOException e) {
            System.out.println("[ERROR] " + e.getMessage());
        }
    }

    private static void uploadJarsToBucket() {
        aws.uploadFileToS3(aws.jarsBucket, "ManagerJar.jar");
        aws.uploadFileToS3(aws.jarsBucket, "WorkerJar.jar");
    }

    private static void receiveMassagesFromSQS(String localSQSURL) {
        List<Message> messages = new ArrayList<>();
        boolean breakTheLoop = false;
        while (!breakTheLoop) {
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(localSQSURL)
                    .waitTimeSeconds(20)
                    .build();
            messages = aws.sqs.receiveMessage(receiveRequest).messages();
            if (!messages.isEmpty()) {
                breakTheLoop = true;
            } else {
                try {
                    Thread.sleep(20000); // Sleep for 30 seconds
                } catch (InterruptedException e) {
                    System.out.println("[ERROR] " + e.getMessage());
                }
            }
        }
        for (Message message : messages) {
            String fileName = message.body();
            processFiles(fileName);
            System.out.println("OutputFile created");
            DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                    .queueUrl(localSQSURL)
                    .receiptHandle(message.receiptHandle())
                    .build();
            aws.sqs.deleteMessage(deleteRequest);
        }
    }

    private static void processFiles(String fileName) {
        String outputFile = "output.html";
        try {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(aws.answersBucket)
                    .key(fileName)
                    .build();
            ResponseInputStream<GetObjectResponse> s3Object = aws.s3.getObject(getObjectRequest);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object))) {
                BufferedWriter writerToHtml = new BufferedWriter(new FileWriter(outputFile));
                writerToHtml.write("<html><head><title>Review Analysis</title></head><body> <h1>Review Analysis</h1>");
                // Process each line
                String line;
                String sentimentColor = "";
                String link = "";
                String entities = "";
                String sarcasm;
                while ((line = reader.readLine()) != null) {
                    if (line.trim().isEmpty()) {
                        // Empty line, add it to the HTML file
                        writerToHtml.write("<br/>");
                    } else {
                        String substring = line.substring(line.indexOf(":") + 2);
                        if (line.startsWith("Sentiment:")) {
                            int sentiment = Integer.parseInt(substring.trim());
                            sentimentColor = getColor(sentiment);
                        } else if (line.startsWith("Link:")) {
                            link = substring.trim();
                        } else if (line.startsWith("Entities:")) {
                            entities = line.substring(line.indexOf("[")).trim();
                        } else if (line.startsWith("Sarcasm:")) {
                            sarcasm = substring.trim();
                            // Generate HTML content for this review
                            writerToHtml.write("<div>");
                            writerToHtml.write("<p>Link: <a href=\"" + link + "\" style=\"color:" + sentimentColor + "\">" + link + "</a></p>");
                            writerToHtml.write("<p>Entities: " + entities.replace(", ", ",") + "</p>");
                            writerToHtml.write("<p>Sarcasm: " + sarcasm + "</p>");
                            writerToHtml.write("</div>");
                            sentimentColor = "";
                            link = "";
                            entities = "";
                        }
                    }
                }
                writerToHtml.write("</body></html>");
                writerToHtml.close();
                System.out.println("HTML file generated successfully!");
            } catch (IOException e) {
                System.out.println("[Debugger log] Failed to read lines from the file, error message:" + e.getMessage());
            }
        } catch (Exception e) {
            System.out.println("[Debugger log] Failed to open the file answer from s3, error message: " + e.getMessage());
        }
    }

    public static String getColor(int sentiment) {
        if (sentiment == 1) {
            return "darkred";
        } else if (sentiment == 2) {
            return "red";
        } else if (sentiment == 3) {
            return "black";
        } else if (sentiment == 4) {
            return "lightgreen";
        } else {
            return "darkgreen";
        }
    }

    private static void deleteBucketAndObjects(String bucketName, boolean deleteBucket) {
        aws.s3.listObjectsV2Paginator(builder -> builder.bucket(bucketName))
                .stream()
                .flatMap(r -> r.contents().stream())
                .forEach(object -> {
                    aws.s3.deleteObject(builder -> builder.bucket(bucketName).key(object.key()));
                });
        try {
            if (deleteBucket) {
                aws.s3.deleteBucket(builder -> builder.bucket(bucketName).build());
                System.out.println("successfully deleted local application bucket");
            }
        } catch (S3Exception e) {
            System.out.println("Failed to delete local application sbucketqs");
            System.err.println("Error deleting bucket: " + e.getMessage());
        }
    }
}
