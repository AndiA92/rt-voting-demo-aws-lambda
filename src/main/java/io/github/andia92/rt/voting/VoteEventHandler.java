package io.github.andia92.rt.voting;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.andia92.rt.voting.functions.VoteListBuilder;
import io.github.andia92.rt.voting.model.Vote;
import lombok.extern.log4j.Log4j;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;

@Log4j
public class VoteEventHandler
        implements RequestHandler<Map<String, Object>, String> {

    private AmazonDynamoDB dynamoDb;
    private static final String REGION = "region";
    private static final String REST_API_PATH = "rest_api_path";
    private static final String TABLE_NAME = "table_name";
    private static final String ATTRIBUTES = "attributes";

    public String handleRequest(Map<String, Object> input, Context context) {
        String region = System.getenv(REGION);
        String restApiPath = System.getenv(REST_API_PATH);
        String tableName = System.getenv(TABLE_NAME);
        String attributes = System.getenv(ATTRIBUTES);
        List<String> columns = Arrays.asList(attributes.split(","));

        this.initDynamoDbClient(region);
        try {
            return retrieveData(restApiPath, tableName, columns);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void initDynamoDbClient(String region) {
        AmazonDynamoDBClientBuilder amazonDynamoDBClientBuilder = AmazonDynamoDBClientBuilder.standard()
                .withRegion(region);
        this.dynamoDb = amazonDynamoDBClientBuilder.build();
    }

    private String retrieveData(String restApiPath, String tableName, List<String> attributesToGet) throws IOException {
        ScanResult scan = dynamoDb.scan(tableName, attributesToGet);
        Function<List<Map<String, AttributeValue>>, List<Vote>> voteListBuilder = new VoteListBuilder();
        List<Vote> votes = voteListBuilder.apply(scan.getItems());
        Map<String, List<Vote>> content = new HashMap<>();
        content.put("content", votes);

        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(content);

        System.out.println("json: " + json);

        HttpClient client = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost(restApiPath);
        post.setEntity(new StringEntity(json));
        client.execute(post);

        return json;
    }


}