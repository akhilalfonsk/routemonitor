package com.causefinder.routemonitor.rest.model;

import lombok.Data;

import java.util.List;

@Data
public class TimeTableResponse {
    private String errormessage;
    private String route;
    private String numberofresults;
    private String stopid;
    private List<TimeTableInfo> results;
    private String errorcode;
    private String timestamp;
}