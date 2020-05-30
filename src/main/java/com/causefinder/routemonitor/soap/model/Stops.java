package com.causefinder.routemonitor.soap.model;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import lombok.Data;

@Data
public class Stops implements Comparable<Stops> {
    @JacksonXmlProperty(localName = "Route")
    private String route;
    @JacksonXmlProperty(localName = "Direction")
    private String direction;
    @JacksonXmlProperty(localName = "SeqNumber")
    private Integer seqNumber;
    @JacksonXmlProperty(localName = "SeqNumberExt")
    private Integer seqNumberExt;
    @JacksonXmlProperty(localName = "StopNumber")
    private Integer stopId;
    @JacksonXmlProperty(localName = "Latitude")
    private String latitude;
    @JacksonXmlProperty(localName = "Longitude")
    private String longitude;
    @JacksonXmlProperty(localName = "Location")
    private String location;
    @JacksonXmlProperty(localName = "Address")
    private String address;

    @Override
    public int compareTo(Stops o) {
        if (!getRoute().equalsIgnoreCase(o.getRoute())) return getRoute().compareTo(o.getRoute());
        else if (!getSeqNumber().equals(o.getSeqNumber())) return getSeqNumber().compareTo(o.getSeqNumber());
        else return getSeqNumberExt().compareTo(getSeqNumberExt());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Stops stops = (Stops) o;

        if (!route.equals(stops.route)) return false;
        if (!direction.equals(stops.direction)) return false;
        if (!seqNumber.equals(stops.seqNumber)) return false;
        if (!seqNumberExt.equals(stops.seqNumberExt)) return false;
        return stopId.equals(stops.stopId);
    }

    @Override
    public int hashCode() {
        int result = route.hashCode();
        result = 31 * result + direction.hashCode();
        result = 31 * result + seqNumber.hashCode();
        result = 31 * result + seqNumberExt.hashCode();
        result = 31 * result + stopId.hashCode();
        return result;
    }
}
