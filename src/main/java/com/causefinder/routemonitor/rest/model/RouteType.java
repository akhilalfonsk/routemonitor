package com.causefinder.routemonitor.rest.model;

import lombok.Data;

import java.util.List;

@Data
public class RouteType {
    private String destinationlocalized;
    private String origin;
    private String originlocalized;
    private String destination;
    private List<StopInfo> stops;
    private String operator;
}
