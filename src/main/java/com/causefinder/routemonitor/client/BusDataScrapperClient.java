package com.causefinder.routemonitor.client;

import com.causefinder.routemonitor.rest.model.RealtimeBusInformationResponse;
import com.causefinder.routemonitor.rest.model.RouteResponse;
import com.causefinder.routemonitor.rest.model.TimeTableResponse;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class BusDataScrapperClient {
    RestTemplate restTemplate = new RestTemplate();

    public RouteResponse getRouteInformation(String operator, String route) {
        String dataUrl = "https://data.smartdublin.ie/cgi-bin/rtpi/routeinformation?operator=" + operator + "&routeid=" + route + "&format=json";
        ResponseEntity<RouteResponse> response = restTemplate.getForEntity(dataUrl, RouteResponse.class);
        return response.getBody();
    }

    public RouteResponse getDefaultRouteInformation(String operator, String route) {
        return getRouteInformation(operator, route);
    }

    public TimeTableResponse getWeeklyTimeTableInformation(String route, String stopId) {
        String dataUrl = "https://data.smartdublin.ie/cgi-bin/rtpi/timetableinformation?type=Week&stopid=" + stopId + "&routeid=" + route + "&format=json";
        ResponseEntity<TimeTableResponse> response = restTemplate.getForEntity(dataUrl, TimeTableResponse.class);
        return response.getBody();
    }

    public RealtimeBusInformationResponse getRealTimeInformation(String stopId) {
        String dataUrl = "https://data.smartdublin.ie/cgi-bin/rtpi/realtimebusinformation?stopid=" + stopId + "&format=json";
        ResponseEntity<RealtimeBusInformationResponse> response = restTemplate.getForEntity(dataUrl, RealtimeBusInformationResponse.class);
        return response.getBody();
    }
}
