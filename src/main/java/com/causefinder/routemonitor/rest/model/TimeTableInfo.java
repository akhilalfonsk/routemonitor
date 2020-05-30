package com.causefinder.routemonitor.rest.model;

import lombok.Data;

import java.util.List;

@Data
public class TimeTableInfo {
    private String destinationlocalized;
    private String enddayofweek;
    private String startdayofweek;
    private String destination;
    private String lastupdated;
    private List<String> departures;
}
