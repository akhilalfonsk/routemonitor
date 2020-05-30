package com.causefinder.routemonitor.soap.model;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import lombok.Data;

import java.util.Date;

@Data
public class StopData {
    @JacksonXmlProperty(localName = "MonitoredVehicleJourney_OperatorRef")
    private String operatorId;
    @JacksonXmlProperty(localName = "MonitoredVehicleJourney_LineRef")
    private String routeId;
    @JacksonXmlProperty(localName = "MonitoredVehicleJourney_DirectionRef")
    private String direction;
    @JacksonXmlProperty(localName = "MonitoredVehicleJourney_DestinationRef")
    private Integer destinationId;
    @JacksonXmlProperty(localName = "MonitoredVehicleJourney_DestinationName")
    private String destinationName;
    @JacksonXmlProperty(localName = "MonitoredStopVisit_MonitoringRef")
    private Integer stopId;
    @JacksonXmlProperty(localName = "FramedVehicleJourneyRef_DataFrameRef")
    private String dataFrameRef;
    @JacksonXmlProperty(localName = "FramedVehicleJourneyRef_DatedVehicleJourneyRef")
    private Integer datedVehicleJourneyRef;
    @JacksonXmlProperty(localName = "MonitoredVehicleJourney_VehicleRef")
    private Integer vehicleRef;
    @JacksonXmlProperty(localName = "MonitoredVehicleJourney_BlockRef")
    private Integer blockRef;
    @JacksonXmlProperty(localName = "MonitoredCall_VisitNumber")
    private Integer visitNumber;

    @JacksonXmlProperty(localName = "MonitoredVehicleJourney_InCongestion")
    private boolean inCongestion;
    @JacksonXmlProperty(localName = "MonitoredCall_VehicleAtStop")
    private boolean vehicleAtStop;

    @JacksonXmlProperty(localName = "MonitoredCall_AimedArrivalTime")
    private Date aimedArrivalTime;
    @JacksonXmlProperty(localName = "MonitoredCall_ExpectedArrivalTime")
    private Date expectedArrivalTime;

    @JacksonXmlProperty(localName = "MonitoredCall_AimedDepartureTime")
    private Date aimedDepartureTime;
    @JacksonXmlProperty(localName = "MonitoredCall_ExpectedDepartureTime")
    private Date expectedDepartureTime;

    @JacksonXmlProperty(localName = "LineNote")
    private String lineNote;

    @JacksonXmlProperty(localName = "ServiceDelivery_ResponseTimestamp")
    private Date recordedAtTime;


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StopData stopData = (StopData) o;

        if (!operatorId.equals(stopData.operatorId)) return false;
        if (!routeId.equals(stopData.routeId)) return false;
        if (!direction.equals(stopData.direction)) return false;
        if (!destinationId.equals(stopData.destinationId)) return false;
        if (!stopId.equals(stopData.stopId)) return false;
        if (!dataFrameRef.equals(stopData.dataFrameRef)) return false;
        if (!datedVehicleJourneyRef.equals(stopData.datedVehicleJourneyRef)) return false;
        return visitNumber.equals(stopData.visitNumber);
    }

    @Override
    public int hashCode() {
        int result = operatorId.hashCode();
        result = 31 * result + routeId.hashCode();
        result = 31 * result + direction.hashCode();
        result = 31 * result + destinationId.hashCode();
        result = 31 * result + stopId.hashCode();
        result = 31 * result + dataFrameRef.hashCode();
        result = 31 * result + datedVehicleJourneyRef.hashCode();
        result = 31 * result + visitNumber.hashCode();
        return result;
    }
}
