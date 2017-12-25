package io.github.andia92.rt.voting.functions;


import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import io.github.andia92.rt.voting.model.Vote;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class VoteListBuilder implements Function<List<Map<String, AttributeValue>>, List<Vote>> {

    @Override
    public List<Vote> apply(List<Map<String, AttributeValue>> items) {
        return items.stream()
                .map(item -> {
                    AttributeValue idAttribute = item.get("candidateId");
                    AttributeValue countAttribute = item.get("count");

                    String candidateId = idAttribute == null ? null : idAttribute.getS();
                    Integer count = countAttribute == null ? 0 : Integer.parseInt(countAttribute.getN());
                    return new Vote(candidateId, count);
                })
                .collect(Collectors.toList());
    }
}
