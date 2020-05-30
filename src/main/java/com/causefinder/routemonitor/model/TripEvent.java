package com.causefinder.routemonitor.model;

import lombok.Data;

import java.time.DayOfWeek;
import java.util.Date;

@Data
public class TripEvent implements Comparable<TripEvent> {
    private String tripEventId;
    private String tripId;
    private Integer tripEventSequenceId;

    private String routeId;
    private String routeTypeId;
    private String routeOperator;
    private String tripOrigin;
    private String tripDestination;

    private String tripScheduledStartTime;
    private Date tripScheduledStartDateTime;
    private String tripScheduledStartPartOfDay;
    private DayOfWeek tripScheduledDayOfWeek;
    private String tripEventType;
    private String tripEventStatus;

    private String previousBusStopId;
    private String previousBusStopName;

    private String busStopId;
    private String busStopName;
    private String busStopLatitude;
    private String busStopLongitude;

    private String nextBusStopId;
    private String nextBusStopName;

    private String scheduledArrivalTime;
    private Date scheduledArrivalDateTime;
    private Date actualArrivalDateTime;
    private Date predictedArrivalDateTime;

    private Long timeSinceTripStartInSeconds;
    private Long actualDelayTimeInSeconds;
    private Long predictedDelayTimeInSeconds;
    private Long deviationInPredictionInSeconds;

    private String delayType;
    private String delayCauseVerbose;

    @Override
    public int compareTo(TripEvent other) {
        return scheduledArrivalTime.compareTo(other.scheduledArrivalTime);
    }
}
