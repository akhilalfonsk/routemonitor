package com.causefinder.routemonitor.model;

import lombok.Data;

import java.util.List;

@Data
public class TripDebug {
    private String tripId;

    private String routeId;
    private String routeTypeId;
    private String routeOperator;
    private String tripOrigin;
    private String tripDestination;

    private List<String> schedules;
}
