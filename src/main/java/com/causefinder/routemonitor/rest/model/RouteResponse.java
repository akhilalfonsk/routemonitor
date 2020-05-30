package com.causefinder.routemonitor.rest.model;

import lombok.Data;

import java.util.List;

@Data
public class RouteResponse {
    private String errormessage;
    private String route;
    private Integer numberofresults;
    private String errorcode;
    private String timestamp;
    private List<RouteType> results;
}