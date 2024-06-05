import com.amazonaws.util.EC2MetadataUtils;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class AWSWorker {
    public final S3Client s3;
    public final SqsClient sqs;
    public final Ec2Client ec2;



    public static Region region1 = Region.US_WEST_2;
    public static Region region2 = Region.US_EAST_1;

    private static final AWSWorker instance = new AWSWorker();

    public String MassagesReceiverSOS;

    public String managerToWorkerSQS;

    public AtomicBoolean terminate;


    private AWSWorker() {
        s3 = S3Client.builder().region(region1).build();
        sqs = SqsClient.builder().region(region2).build();
        ec2 = Ec2Client.builder().region(region2).build();
        managerToWorkerSQS = "managerToWorkerSQS";
        MassagesReceiverSOS = "MassagesReceiverSQS";
        terminate = new AtomicBoolean(false);

    }

    public static AWSWorker getInstance() {
        return instance;
    }


    public String CheckSQS(String sqsName) {
        try {
            GetQueueUrlResponse getQueueUrlResponse = sqs.getQueueUrl(GetQueueUrlRequest.builder()
                    .queueName(sqsName)
                    .build());
            return getQueueUrlResponse.queueUrl();

        } catch (QueueDoesNotExistException e) {
            CreateQueueResponse createQueueResponse = sqs.createQueue(CreateQueueRequest.builder()
                    .queueName(sqsName)
                    .build());
            return createQueueResponse.queueUrl();
        }
    }

    public List<Message> GetFromManager(String sqsUrl) {
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(sqsUrl)
                .visibilityTimeout(10)
                .messageAttributeNames("All")
                .maxNumberOfMessages(1)
                .build();
        return sqs.receiveMessage(receiveRequest).messages();
    }

    public void SendToManagerSQS(String sqsUrl, String localSqsUrl, String response) {
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(sqsUrl)
                .messageBody(response)
                .messageAttributes(
                        Map.of(
                                "localSQSUrl", MessageAttributeValue.builder().dataType("String").stringValue(localSqsUrl).build()
                        )
                )
                .build();
        sqs.sendMessage(send_msg_request);
    }

    public void deleteMessageFromManagerToWorkerSQS(String managerToWorkerURL, Message message) {
        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(managerToWorkerURL)
                .receiptHandle(message.receiptHandle())
                .build();
        sqs.deleteMessage(deleteRequest);
    }

    public void changeMessageVisibilityRequest(String queueUrl, String receiptHandle) {
        sqs.changeMessageVisibility(ChangeMessageVisibilityRequest.builder()
                .queueUrl(queueUrl)
                .visibilityTimeout(10)
                .receiptHandle(receiptHandle)
                .build());
    }

    public void shutdownInstance() {
        TerminateInstancesRequest request = TerminateInstancesRequest.builder()
                .instanceIds(EC2MetadataUtils.getInstanceId())
                .build();
        ec2.terminateInstances(request);
    }
}
