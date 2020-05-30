package com.causefinder.routemonitor.util;

import com.causefinder.routemonitor.model.Trip;
import com.causefinder.routemonitor.model.TripDebug;
import org.modelmapper.ModelMapper;
import org.springframework.stereotype.Component;

import java.util.stream.Collectors;

@Component
public class TripConverter {

    private ModelMapper modelMapper = new ModelMapper();

    public TripDebug convertToDebug(Trip trip) {
        TripDebug tripDebug = modelMapper.map(trip, TripDebug.class);
        tripDebug.setSchedules(trip.getScheduledEvents().stream().map(tripEvent -> tripEvent.getBusStopId() + "->" + tripEvent.getScheduledArrivalTime()).collect(Collectors.toList()));
        return tripDebug;
    }
}
