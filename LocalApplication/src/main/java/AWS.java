import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

public class AWS {
    public final S3Client s3;
    public final SqsClient sqs;
    public final Ec2Client ec2;

    public String globalSQS;

    public static Region region1 = Region.US_WEST_2;
    public static Region region2 = Region.US_EAST_1;

    private static final AWS instance = new AWS();

    public String bucketName = "bucket" + System.currentTimeMillis();

    public String answersBucket = "bucketoutputfiless";

    public String jarsBucket = "bucketjarsbucketshacharyovel";

    public String managerTag = "ManagerJar.jar";

    public static String ami = "ami-00e95a9222311e8ed";

    public String managerInstanceId = "";

    public List<String> inputFiles;
    public String outputFile;
    public String n;

    public String terminate;

    private AWS() {
        s3 = S3Client.builder().region(region1).build();
        sqs = SqsClient.builder().region(region2).build();
        ec2 = Ec2Client.builder().region(region2).build();
        globalSQS = "globalSQS";
        inputFiles = new ArrayList<>();
        outputFile = "";
        n = "";
        terminate = "";
    }

    public static AWS getInstance() {
        return instance;
    }

    public void createManagerIfNotExists() {
        if (!checkIfManagerExist()) {
            String managerScript = "#!/bin/bash\n" +
                    "sudo yum update -y \n" +
                    "curl -s \"https://get.sdkman.io\" | bash\n" +
                    "source \"$HOME/.sdkman/bin/sdkman-init.sh\"\n" +
                    "sdk install java 17.0.1-open\n" +
                    "sdk use java 17.0.1-open\n" +
                    "sudo mkdir ManagerPath\n" +
                    "sudo aws s3 cp s3://" + jarsBucket + "/" + managerTag +" ./ManagerPath/" + "ManagerJar.jar" +"\n"+
                    "java -jar /ManagerPath/ManagerJar.jar" + "\n";
            createManager(managerScript);
        }
    }

    public void createManager(String script) {
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(InstanceType.M4_LARGE)
                .imageId(ami)
                .maxCount(1)
                .minCount(1)
                .keyName("vockey")
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                .userData(Base64.getEncoder().encodeToString((script).getBytes()))
                .build();
        RunInstancesResponse response = ec2.runInstances(runRequest);

        String instanceId = response.instances().get(0).instanceId();

        software.amazon.awssdk.services.ec2.model.Tag tag = Tag.builder()
                .key("Name")
                .value("Manager")
                .build();

        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);
            System.out.printf(
                    "started EC2 instance %s based on AMI %s\n",
                    instanceId, ami);

        } catch (Ec2Exception e) {
            System.err.println("[ERROR] " + e.getMessage());
            System.exit(1);
        }
        managerInstanceId = instanceId;
    }

    public void createBucketIfNotExists(String bucketName) {
        try {
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucketName)
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                    .locationConstraint(BucketLocationConstraint.US_WEST_2)
                                    .build())
                    .build());
            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
        } catch (S3Exception e) {
            System.out.println("[ERROR] " + e.getMessage());
        }
    }

    public void uploadFileToS3(String bucketName, String fileName) {
        Path filePath = Paths.get(fileName);
        String key = filePath.getFileName().toString();
        s3.putObject(PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build(), RequestBody.fromFile(filePath));
    }

    public String checkGlobalSQS() {
        try {
            GetQueueUrlResponse getQueueUrlResponse = sqs.getQueueUrl(GetQueueUrlRequest.builder()
                    .queueName(globalSQS)
                    .build());

            return getQueueUrlResponse.queueUrl();
        } catch (QueueDoesNotExistException e) {
            CreateQueueResponse createQueueResponse = sqs.createQueue(CreateQueueRequest.builder()
                    .queueName(globalSQS)
                    .build());
            return createQueueResponse.queueUrl();
        }
    }

    public String createLocalSQS(String queueName) {
        CreateQueueResponse createQueueResponse = sqs.createQueue(CreateQueueRequest.builder()
                .queueName(queueName)
                .build());
        return createQueueResponse.queueUrl();
    }

    public void sendToGlobalSQS(String globalSQSUrl, String localSQSUrl) {
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(globalSQSUrl)
                .messageBody(localSQSUrl)
                .messageAttributes(
                        Map.of(
                                "Bucket", MessageAttributeValue.builder().dataType("String").stringValue(bucketName).build(),
                                "File", MessageAttributeValue.builder().dataType("String").stringValue("combinedFiles.txt").build(),
                                "n", MessageAttributeValue.builder().dataType("String").stringValue(n).build(),
                                "Terminate", MessageAttributeValue.builder().dataType("String").stringValue(terminate).build()
                        )
                )
                .delaySeconds(20)
                .build();
        sqs.sendMessage(send_msg_request);
    }

    public void deleteLocalSQS(String sqsUrl) {
        DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                .queueUrl(sqsUrl)
                .build();
        DeleteQueueResponse deleteQueueResponse = sqs.deleteQueue(deleteQueueRequest);
        if (deleteQueueResponse.sdkHttpResponse().isSuccessful()) {
            System.out.println("successfully deleted local application sqs\n");
        } else {
            System.out.println("Failed to delete local application sqs");
        }
    }

    private boolean checkIfManagerExist() {
        String nextToken = null;
        do {
            DescribeInstancesRequest request = DescribeInstancesRequest
                    .builder().nextToken(nextToken).build();
            DescribeInstancesResponse response = ec2.describeInstances(request);

            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    if (instance.state().name().toString().equals("running")) {
                        for (software.amazon.awssdk.services.ec2.model.Tag tag : instance.tags()) {
                            if (tag.key().equals("Name") && tag.value().equals("Manager")) {
                                return true;
                            }
                        }
                    }
                }
            }
            nextToken = response.nextToken();
        } while (nextToken != null);
        return false;
    }

    public void deleteBucketAndObjects(String bucketName, boolean deleteBucket) {
        s3.listObjectsV2Paginator(builder -> builder.bucket(bucketName))
                .stream()
                .flatMap(r -> r.contents().stream())
                .forEach(object -> s3.deleteObject(builder -> builder.bucket(bucketName).key(object.key())));
        try {
            if (deleteBucket) {
                s3.deleteBucket(builder -> builder.bucket(bucketName).build());
                System.out.println("successfully deleted local application bucket");
            }
        } catch (S3Exception e) {
            System.out.println("Failed to delete local application sbucketqs");
            System.err.println("Error deleting bucket: " + e.getMessage());
        }
    }

    public void deleteMessageFromQueue(String queueUrl, Message message) {
        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqs.deleteMessage(deleteRequest);
    }
}