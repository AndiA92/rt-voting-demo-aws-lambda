package io.github.andia92.rt.voting;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import lombok.extern.log4j.Log4j;
import java.io.IOException;
import java.util.*;

@Log4j
public class VoteProducer
        implements RequestHandler<Map<String, Object>, String> {

    private AmazonDynamoDB dynamoDb;
    private static final String TABLE_NAME = "table_name";
    private static final String REGION = "region";
    private final List<String> names = Arrays.asList("A", "B", "C", "D", "E");

    public String handleRequest(Map<String, Object> input, Context context) {
        String region = System.getenv(REGION);
        String tableName = System.getenv(TABLE_NAME);
        this.initDynamoDbClient(region);
        try {
            return writeData(tableName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void initDynamoDbClient(String region) {
        AmazonDynamoDBClientBuilder amazonDynamoDBClientBuilder = AmazonDynamoDBClientBuilder.standard()
                                                                                             .withRegion(region);
        this.dynamoDb = amazonDynamoDBClientBuilder.build();
    }

    private String writeData(String tableName) throws IOException {
        Map<String, AttributeValue> data = new HashMap<>();
        Random random = new Random();
        for (int i = 0; i < 60; i++) {
            data.clear();
            AttributeValue candidate = new AttributeValue("Candidate " + generateName());
            AttributeValue count = new AttributeValue();
            count.setN(Integer.toString(generateValue()));

            data.put("candidateId", candidate);
            data.put("count", count);

            dynamoDb.putItem(tableName, data);
            try {
                Thread.sleep(1000 * random.nextInt(10));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        return "success";
    }

    private String generateName() {
        Random random = new Random();
        int randomValue = random.nextInt(5);
        return names.get(randomValue);
    }

    private int generateValue() {
        Random random = new Random();
        return random.nextInt(100);
    }


}