package com.causefinder.routemonitor.rest.model;

import lombok.Data;

import java.util.List;

@Data
public class OperatorInfo {
    private List<String> routes;
    private String operatortype;
    private String name;
}
