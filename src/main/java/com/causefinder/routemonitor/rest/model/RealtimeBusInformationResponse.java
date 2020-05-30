package com.causefinder.routemonitor.rest.model;

import java.sql.Timestamp;
import java.util.List;

public class RealtimeBusInformationResponse {
    private String errormessage;
    private String numberofresults;
    private String stopid;
    private List<RealTimeInfo> results;
    private String errorcode;
    private Timestamp timestamp;
}
