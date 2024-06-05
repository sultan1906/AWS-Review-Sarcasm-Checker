import com.amazonaws.util.EC2MetadataUtils;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.utils.Pair;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class AWSManager {
    public final S3Client s3;
    public final SqsClient sqs;
    public final Ec2Client ec2;

    public String globalSQS;

    public String managerToWorkerSQS;

    public static Region region1 = Region.US_WEST_2;
    public static Region region2 = Region.US_EAST_1;

    private static final AWSManager instance = new AWSManager();

    public String bucketName = "bucketoutputfiless";

    public String MassagesReceiverSOS;

    public ConcurrentLinkedQueue<Message> QueueOfRequests;

    public ConcurrentHashMap<String, Integer> MapOfReviews;

    ConcurrentHashMap<String, String> MapOfNameFiles = new ConcurrentHashMap<>();

    public AtomicBoolean terminate;

    public AtomicInteger filesCounter;

    public AtomicInteger WorkersCounter;

    public String managerTag = "ManagerJar.jar";
    public String workerTag = "WorkerJar.jar";

    public String ami = "ami-00e95a9222311e8ed";

    public String jarsBucket = "bucketjarsbucketshacharyovel";

    final public Object updateWorkersLock = new Object();

    final public Object receivingMessagesFromWorkers = new Object();
    final public Object createFilesLock = new Object();
    final public Object NumOfReviewsLock = new Object();


    private AWSManager() {
        s3 = S3Client.builder().region(region1).build();
        sqs = SqsClient.builder().region(region2).build();
        ec2 = Ec2Client.builder().region(region2).build();
        globalSQS = "globalSQS";
        managerToWorkerSQS = "managerToWorkerSQS";
        MassagesReceiverSOS = "MassagesReceiverSQS";
        QueueOfRequests = new ConcurrentLinkedQueue<>();
        MapOfReviews = new ConcurrentHashMap<>();
        terminate = new AtomicBoolean(false);
        filesCounter = new AtomicInteger(1);
        WorkersCounter = new AtomicInteger(0);

    }

    public static AWSManager getInstance() {
        return instance;
    }

    public void createWorkers(int numOfWorkers) {
        String managerScript = "#!/bin/bash\n" +
                "sudo yum update -y \n" +
                "curl -s \"https://get.sdkman.io\" | bash\n" +
                "source \"$HOME/.sdkman/bin/sdkman-init.sh\"\n" +
                "sdk install java 17.0.1-open\n" +
                "sdk use java 17.0.1-open\n" +
                "'Creating directory WorkerFiles'\n" +
                "sudo mkdir WorkersPath\n" +
                "'Downloading WorkerJar from S3 bucket'\n" +
                "sudo aws s3 cp s3://" + jarsBucket + "/" + workerTag +" ./WorkersPath/" + "WorkerJar.jar" +"\n"+
                "'Running WorkerJar program'\n" +
                "java -jar /WorkersPath/WorkerJar.jar" +"\n";
        createWorkers(managerScript, numOfWorkers);
    }

    public void createWorkers(String script, int numOfWorkers) {
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(InstanceType.M4_LARGE)
                .imageId(ami)
                .maxCount(numOfWorkers)
                .minCount(1)
                .keyName("vockey")
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                .userData(Base64.getEncoder().encodeToString((script).getBytes()))
                .build();
        RunInstancesResponse response = ec2.runInstances(runRequest);

        Collection<String> instanceIDs = response.instances().stream().map(Instance::instanceId).collect(Collectors.toList());

        Tag tag = Tag.builder()
                .key("Name")
                .value("Worker")
                .build();

        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceIDs)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);

        } catch (Ec2Exception e) {
            System.err.println("[ERROR] " + e.getMessage());
        }
    }

    public String CheckGlobalSQS(){
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

    public String CheckAndCreateManagerToWorkerSQS() {
        try {
            GetQueueUrlResponse getQueueUrlResponse = sqs.getQueueUrl(GetQueueUrlRequest.builder()
                    .queueName(managerToWorkerSQS)
                    .build());

            return getQueueUrlResponse.queueUrl();

        } catch (QueueDoesNotExistException e) {
            CreateQueueResponse createQueueResponse = sqs.createQueue(CreateQueueRequest.builder()
                    .queueName(managerToWorkerSQS)
                    .build());
            return createQueueResponse.queueUrl();
        }
    }

    public void createOutputFilesBucketBucketIfNotExists() {
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
            System.out.println(e.getMessage());
        }
    }

    public String CheckMassagesReceiverSOS(){
        try {
            GetQueueUrlResponse getQueueUrlResponse = sqs.getQueueUrl(GetQueueUrlRequest.builder()
                    .queueName(MassagesReceiverSOS)
                    .build());
            return getQueueUrlResponse.queueUrl();
        } catch (QueueDoesNotExistException e) {
            CreateQueueResponse createQueueResponse = sqs.createQueue(CreateQueueRequest.builder()
                    .queueName(MassagesReceiverSOS)
                    .build());
            return createQueueResponse.queueUrl();
        }
    }

    public void shutdownInstance() {
        try {
            TerminateInstancesRequest request = TerminateInstancesRequest.builder()
                    .instanceIds(EC2MetadataUtils.getInstanceId())
                    .build();
            ec2.terminateInstances(request);
        } catch (Exception e) {
            System.out.println("[ERROR] " + e.getMessage());
        }
    }

    public List<Message> GlobalSQSMessages(String globalSQSURL){
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(globalSQSURL)
                .messageAttributeNames("All")
                .maxNumberOfMessages(1)
                .build();
        List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
        return messages;
    }

    public void SendToManagerToWorkerSQS(String localSQSUrl, String sqsManagerToWorker,
                                          Pair<String, String> urlLinkReview, String rating) {
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(sqsManagerToWorker)
                .messageBody(urlLinkReview.right())
                .messageAttributes(
                        Map.of(
                                "Link", MessageAttributeValue.builder().dataType("String").stringValue(urlLinkReview.left()).build(),
                                "Rating", MessageAttributeValue.builder().dataType("String").stringValue(rating).build(),
                                "SQSLocalUrl", MessageAttributeValue.builder().dataType("String").stringValue(localSQSUrl).build()
                        )
                )
                .delaySeconds(10)
                .build();
        sqs.sendMessage(send_msg_request);
    }

    public void deleteMessageFromGlobalSqs(String sqsUrl, Message message) {
        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(sqsUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqs.deleteMessage(deleteRequest);
    }

    public int checkHowManyWorkersRunning() {
        String nextToken = null;
        int counter = 0;
        do {
            DescribeInstancesRequest request = DescribeInstancesRequest
                    .builder().nextToken(nextToken).build();
            DescribeInstancesResponse response = ec2.describeInstances(request);

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
}