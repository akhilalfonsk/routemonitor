package com.causefinder.routemonitor.client;

import com.causefinder.routemonitor.soap.model.StopEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.Timestamp;
import com.google.cloud.bigquery.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class BigQueryClient {

    @Value("${gcp.tripdb.dataset}")
    private String datasetName;
    @Value("${gcp.tripdb.table}")
    private String tableName;

    @Autowired
    private BigQuery bigQueryClient;

    private ObjectMapper oMapper = new ObjectMapper();

    public void pushDataToGCP(List<StopEvent> stopEvents) {
        TableId tableId = TableId.of(datasetName, tableName);
        InsertAllRequest.Builder builder = InsertAllRequest.newBuilder(tableId);
        stopEvents.stream().forEach(stopEvent -> {
            Map<String, Object> datMap = oMapper.convertValue(stopEvent, Map.class);
            datMap.put("aimedArrivalTime", Timestamp.of(stopEvent.getAimedArrivalTime()).toString());
            datMap.put("expectedArrivalTime", Timestamp.of(stopEvent.getExpectedArrivalTime()).toString());
            datMap.put("aimedDepartureTime", Timestamp.of(stopEvent.getAimedDepartureTime()).toString());
            datMap.put("expectedDepartureTime", Timestamp.of(stopEvent.getExpectedDepartureTime()).toString());
            datMap.put("recordedAtTime", Timestamp.of(stopEvent.getRecordedAtTime()).toString());
            builder.addRow(datMap);
        });
        InsertAllResponse response = bigQueryClient.insertAll(builder.build());
        if (response.hasErrors()) {
            for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                StringBuilder errorString = new StringBuilder("Error in Pushing data to GCP:");
                errorString.append("[").append(entry.getKey()).append("=>[");
                entry.getValue().stream().forEach(err -> errorString.append(err.getMessage()).append(","));
                errorString.append("]]");
                log.warn("Error in Pushing data to GCP: {} ", errorString.toString());
            }
        }
    }

}
