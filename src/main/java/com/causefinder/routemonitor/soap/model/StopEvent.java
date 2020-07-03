package com.causefinder.routemonitor.soap.model;

import lombok.Data;

import java.util.Date;


@Data
public class StopEvent {
    private String operatorId;
    private String routeId;
    private String direction;
    private Integer destinationId;
    private String destinationName;
    private Integer stopId;
    private String dataFrameRef;
    private Integer datedVehicleJourneyRef;
    private Integer vehicleRef;
    private Integer blockRef;
    private Integer visitNumber;
    private Integer seqNumber;
    private boolean inCongestion;
    private boolean vehicleAtStop;
    private Date aimedArrivalTime;
    private Date expectedArrivalTime;
    private Date aimedDepartureTime;
    private Date expectedDepartureTime;
    private String latitude;
    private String longitude;
    private String location;
    private String address;

 //   private String lineNote;
    private Date recordedAtTime;
}
