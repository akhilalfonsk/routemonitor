package com.causefinder.routemonitor.rest.model;

import lombok.Data;

import java.time.DayOfWeek;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

@Data
public class StopInfo {
    private String fullnamelocalized;
    private String latitude;
    private String stopid;
    private String fullname;
    private String shortnamelocalized;
    private String shortname;
    private String displaystopid;
    private String longitude;
    private List<OperatorInfo> operators;
    private Map<DayOfWeek, PriorityQueue<String>> schedules = new HashMap<>();
}
