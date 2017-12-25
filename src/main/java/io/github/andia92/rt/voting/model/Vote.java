package io.github.andia92.rt.voting.model;

import lombok.Data;

@Data
public class Vote {

    private final String label;

    private final int value;
}
