package com.causefinder.routemonitor.model;

import lombok.Data;

import java.util.Date;
import java.util.LinkedList;
import java.util.Objects;

@Data
public class Trip implements Comparable<Trip> {

    private String tripId;

    private String routeId;
    private String routeTypeId;
    private String routeOperator;
    private String tripOrigin;
    private String tripDestination;

    private Boolean tripStarted;
    private String tripStartTime;
    private Date tripStartDateTime;

    private LinkedList<TripEvent> scheduledEvents;

    @Override
    public int compareTo(Trip other) {
        if (Objects.nonNull(tripStartDateTime) && Objects.nonNull(other.tripStartDateTime)) {
            return tripStartDateTime.compareTo(other.tripStartDateTime);
        } else if (Objects.nonNull(tripStartTime) && Objects.nonNull(other.tripStartTime)) {
            return tripStartTime.compareTo(other.tripStartTime);
        } else {
            return routeTypeId.compareTo(other.routeTypeId);
        }
    }
}
