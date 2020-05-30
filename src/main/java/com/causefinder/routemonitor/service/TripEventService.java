package com.causefinder.routemonitor.service;

import com.causefinder.routemonitor.client.BusDataScrapperClient;
import com.causefinder.routemonitor.exception.NoRouteResponse;
import com.causefinder.routemonitor.exception.NoTimeTableResponse;
import com.causefinder.routemonitor.model.Trip;
import com.causefinder.routemonitor.model.TripDebug;
import com.causefinder.routemonitor.model.TripEvent;
import com.causefinder.routemonitor.rest.model.*;
import com.causefinder.routemonitor.util.TripConverter;
import lombok.extern.slf4j.Slf4j;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class TripEventService {
    private static SimpleDateFormat tripPatternFormatter = new SimpleDateFormat("yy-ww-uu");

    @Autowired
    private BusDataScrapperClient scrapperClient;
    @Autowired
    private TripConverter tripConverter;

    private ModelMapper modelMapper = new ModelMapper();

    public Map<DayOfWeek, Map<String, PriorityQueue<Trip>>> getWeeklyTripEvents(String routeId) {
        Map<DayOfWeek, Map<String, PriorityQueue<Trip>>> tripSchedules = new HashMap<>();
        try {
            RouteResponse routeResponse = scrapperClient.getDefaultRouteInformation("bac", routeId);
            if (Objects.nonNull(routeResponse) && routeResponse.getNumberofresults() > 0) {
                routeResponse.getResults().forEach(routeType -> findRouteSchedules(routeId, routeType));
                for (DayOfWeek weekDay : DayOfWeek.values()) {
                    tripSchedules.put(weekDay, createDailyTripSchedulesForRouteTypes(weekDay, routeResponse));
                }
            } else {
                throw new NoRouteResponse("No Route Information received from server. RouteId:" + routeId);
            }
        } catch (Exception e) {
            log.error("Error in getWeeklyTripEvents()", e);
        }
        return tripSchedules;
    }

    public Map<DayOfWeek, Map<String, PriorityQueue<Trip>>> getDailyTripEvents(String routeId, DayOfWeek dayOfWeek) {
        Map<DayOfWeek, Map<String, PriorityQueue<Trip>>> tripSchedules = new HashMap<>();
        try {
            RouteResponse routeResponse = scrapperClient.getDefaultRouteInformation("bac", routeId);
            if (Objects.nonNull(routeResponse) && routeResponse.getNumberofresults() > 0) {
                routeResponse.getResults().forEach(routeType -> findRouteSchedules(routeId, routeType));
                tripSchedules.put(dayOfWeek, createDailyTripSchedulesForRouteTypes(dayOfWeek, routeResponse));
            } else {
                throw new NoRouteResponse("No Route Information received from server. RouteId:" + routeId);
            }
        } catch (Exception e) {
            log.error("Error in getWeeklyTripEvents()", e);
        }
        return tripSchedules;
    }

    public Map<DayOfWeek, Map<String, List<TripDebug>>> getDailyTripEventsDebug(String routeId, DayOfWeek dayOfWeek) {
        Map<DayOfWeek, Map<String, List<TripDebug>>> tripSchedules = new HashMap<>();
        try {
            RouteResponse routeResponse = scrapperClient.getDefaultRouteInformation("bac", routeId);
            if (Objects.nonNull(routeResponse) && routeResponse.getNumberofresults() > 0) {
                routeResponse.getResults().forEach(routeType -> findRouteSchedules(routeId, routeType));
                tripSchedules.put(dayOfWeek, createDailyTripSchedulesForRouteTypesDebug(dayOfWeek, routeResponse));
            } else {
                throw new NoRouteResponse("No Route Information received from server. RouteId:" + routeId);
            }
        } catch (Exception e) {
            log.error("Error in getWeeklyTripEvents()", e);
        }
        return tripSchedules;
    }

    public RouteResponse getRoutesWithSchedules(String operator, String routeId) {
        RouteResponse routeResponse = null;
        try {
            routeResponse = scrapperClient.getDefaultRouteInformation(operator, routeId);
            if (Objects.nonNull(routeResponse) && routeResponse.getNumberofresults() > 0) {
                routeResponse.getResults().forEach(routeType -> findRouteSchedules(routeId, routeType));
            } else {
                throw new NoRouteResponse("No Route Information received from server. RouteId:" + routeId);
            }
        } catch (Exception e) {
            log.error("Error in getRoutesWithScheduled()", e);
        }
        return routeResponse;
    }

    private Map<String, PriorityQueue<Trip>> createDailyTripSchedulesForRouteTypes(DayOfWeek weekDay, RouteResponse routeResponse) {
        int routeTypeId = 0;
        Map<String, PriorityQueue<Trip>> routeTypeTripMap = new HashMap<>();
        for (RouteType routeType : routeResponse.getResults()) {
            PriorityQueue<Trip> tripsOfThisRouteType = new PriorityQueue<>();
            boolean firstStop = true;
            for (StopInfo stop : routeType.getStops()) {
                if (firstStop) {
                    tripsOfThisRouteType = createTripsFromFirstStop(weekDay, routeResponse, routeType, ++routeTypeId, stop);
                    firstStop = false;
                } else {
                    tripsOfThisRouteType.parallelStream().forEach(trip -> addMatchingTripEventsForThisStop(weekDay, trip, stop));
                }
            }
            if (!tripsOfThisRouteType.isEmpty())
                routeTypeTripMap.put(tripsOfThisRouteType.peek().getRouteTypeId(), tripsOfThisRouteType);
        }
        return routeTypeTripMap;
    }

    private Map<String, List<TripDebug>> createDailyTripSchedulesForRouteTypesDebug(DayOfWeek weekDay, RouteResponse routeResponse) {
        int routeTypeId = 0;
        Map<String, List<TripDebug>> routeTypeTripMap = new HashMap<>();
        for (RouteType routeType : routeResponse.getResults()) {
            PriorityQueue<Trip> tripsOfThisRouteType = new PriorityQueue<>();
            boolean firstStop = true;
            for (StopInfo stop : routeType.getStops()) {
                if (firstStop) {
                    tripsOfThisRouteType = createTripsFromFirstStop(weekDay, routeResponse, routeType, ++routeTypeId, stop);
                    firstStop = false;
                } else {
                    tripsOfThisRouteType.parallelStream().forEach(trip -> addMatchingTripEventsForThisStop(weekDay, trip, stop));
                }
            }
            if (!tripsOfThisRouteType.isEmpty())
                routeTypeTripMap.put(tripsOfThisRouteType.peek().getRouteTypeId(), tripsOfThisRouteType.stream().map(tripConverter::convertToDebug).collect(Collectors.toList()));
        }
        return routeTypeTripMap;
    }

    private void addMatchingTripEventsForThisStop(DayOfWeek weekDay, Trip trip, StopInfo stopInfo) {
        //log.info("Processing Trip:"+trip.getTripId());
        if (Objects.nonNull(stopInfo.getSchedules().get(weekDay)) && !stopInfo.getSchedules().get(weekDay).isEmpty()) {
            TripEvent newTripEvent = new TripEvent();

            TripEvent lastValidTripEvent = getLastValidTrip(trip, trip.getScheduledEvents().getLast());
            String matchingSchedule = getMatchingScheduleV2(lastValidTripEvent, stopInfo.getSchedules().get(weekDay));

            modelMapper.map(trip.getScheduledEvents().getLast(), newTripEvent);

            newTripEvent.setTripEventSequenceId(trip.getScheduledEvents().size() + 1);
            newTripEvent.setTripEventId(createTripEventId(newTripEvent.getTripId(), newTripEvent.getTripEventSequenceId()));

            newTripEvent.setTripEventType("INTERMEDIATE");

            newTripEvent.setBusStopId(stopInfo.getStopid());
            newTripEvent.setBusStopName(stopInfo.getShortname());
            newTripEvent.setBusStopLatitude(stopInfo.getLatitude());
            newTripEvent.setBusStopLongitude(stopInfo.getLongitude());

            trip.getScheduledEvents().getLast().setNextBusStopId(stopInfo.getStopid());
            trip.getScheduledEvents().getLast().setNextBusStopName(stopInfo.getShortname());

            newTripEvent.setPreviousBusStopId(trip.getScheduledEvents().getLast().getBusStopId());
            newTripEvent.setPreviousBusStopName(trip.getScheduledEvents().getLast().getBusStopName());

            if (Objects.nonNull(matchingSchedule)) {
                newTripEvent.setScheduledArrivalTime(matchingSchedule);
            } else {
                newTripEvent.setScheduledArrivalTime("SKIP");
            }
            log.info("Completed Processing Trip Event:" + newTripEvent.getTripEventId());
            trip.getScheduledEvents().addLast(newTripEvent);
        }
    }

    private TripEvent getLastValidTrip(Trip trip, TripEvent lastTripEvent) {
        int reverseIndexPos = 2;
        while ("SKIP".equalsIgnoreCase(lastTripEvent.getScheduledArrivalTime())) {
            lastTripEvent = trip.getScheduledEvents().get(trip.getScheduledEvents().size() - reverseIndexPos++);
        }
        return lastTripEvent;
    }

    private String getMatchingScheduleV2(TripEvent lastTripEvent, PriorityQueue<String> schedules) {
        String mostMatchedSchedule = null;
        int latestTripInMin = convertToMinFromMidnight(lastTripEvent.getScheduledArrivalTime());
        for (String currSchedule : schedules) {
            int estimatedTimeBetween = convertToMinFromMidnight(currSchedule) - latestTripInMin;
            if (estimatedTimeBetween >= 1 && estimatedTimeBetween <= 30) {
                if (Objects.isNull(mostMatchedSchedule)) {
                    mostMatchedSchedule = currSchedule;
                } else {
                    if (convertToMinFromMidnight(mostMatchedSchedule) - convertToMinFromMidnight(currSchedule) > 0) {
                        mostMatchedSchedule = currSchedule;
                    }
                }
            }
        }
        return mostMatchedSchedule;
    }

    private int convertToMinFromMidnight(String timeIn24HrFormat) {
        int minFromMidNight = 60 * Integer.parseInt(timeIn24HrFormat.trim().split(":")[0]);
        minFromMidNight += Integer.parseInt(timeIn24HrFormat.trim().split(":")[1]);
        return minFromMidNight;
    }

    private String getMatchingSchedule(TripEvent lastTripEvent, PriorityQueue<String> schedules) {
        String mostMatchedSchedule = null;
        Calendar lastEventTime = convertScheduleToCalender(lastTripEvent.getScheduledArrivalTime());
        Map<Calendar, String> schedulesMap = new HashMap<>();
        for (String schedule : schedules) {
            schedulesMap.put(convertScheduleToCalender(schedule), schedule);
        }
        Calendar nearestSchedule = null;
        for (Calendar currSchedule : schedulesMap.keySet()) {
            Long estimatedTimeBetween = ChronoUnit.MINUTES.between(lastEventTime.toInstant(), currSchedule.toInstant());
            if (estimatedTimeBetween >= 1 && estimatedTimeBetween <= 30) {
                if (Objects.isNull(nearestSchedule)) {
                    nearestSchedule = currSchedule;
                } else {
                    if (nearestSchedule.compareTo(currSchedule) < 0) {
                        nearestSchedule = currSchedule;
                    }
                }
            }
        }
        if (Objects.nonNull(nearestSchedule)) {
            mostMatchedSchedule = schedulesMap.get(nearestSchedule);
        }
        return mostMatchedSchedule;
    }

    private Calendar convertScheduleToCalender(String schedule) {
        Calendar scheduleTime = Calendar.getInstance();
        scheduleTime.set(Calendar.HOUR_OF_DAY, Integer.parseInt(schedule.split(":")[0].trim()));
        scheduleTime.set(Calendar.MINUTE, Integer.parseInt(schedule.split(":")[1].trim()));
        scheduleTime.set(Calendar.SECOND, 0);
        return scheduleTime;
    }

    private PriorityQueue<Trip> createTripsFromFirstStop(DayOfWeek weekDay, RouteResponse routeResponse, RouteType routeType, int routeTypeId, StopInfo startStop) {
        PriorityQueue<Trip> trips = new PriorityQueue<>();
        PriorityQueue<String> schedules = startStop.getSchedules().get(weekDay);
        int tripCounter = 1;
        if (Objects.nonNull(schedules)) {
            while (!schedules.isEmpty()) {
                Trip trip = new Trip();
                trip.setTripId(createDummyTripId(weekDay, routeResponse, routeType, routeTypeId, tripCounter++, startStop));
                trip.setRouteId(routeResponse.getRoute());
                trip.setRouteTypeId(routeResponse.getRoute() + "-" + routeTypeId);
                trip.setRouteOperator(routeType.getOperator());
                trip.setTripStarted(Boolean.FALSE);
                trip.setTripOrigin(routeType.getOrigin());
                trip.setTripDestination(routeType.getDestination());
                trip.setScheduledEvents(new LinkedList<>());
                trip.getScheduledEvents().add(createInitialTripEvent(weekDay, trip, startStop));
                trip.setTripStartTime(trip.getScheduledEvents().getLast().getTripScheduledStartTime());
                trips.add(trip);
            }
        }
        return trips;
    }

    private TripEvent createInitialTripEvent(DayOfWeek weekDay, Trip trip, StopInfo startStop) {
        TripEvent initialEvent = new TripEvent();
        initialEvent.setTripEventId(createTripEventId(trip.getTripId(), 1));
        initialEvent.setTripId(trip.getTripId());
        initialEvent.setTripEventSequenceId(1);

        initialEvent.setRouteId(trip.getRouteId());
        initialEvent.setRouteTypeId(trip.getRouteTypeId());
        initialEvent.setRouteOperator(trip.getRouteOperator());
        initialEvent.setTripOrigin(trip.getTripOrigin());
        initialEvent.setTripDestination(trip.getTripDestination());

        initialEvent.setTripScheduledStartTime(startStop.getSchedules().get(weekDay).poll());
        initialEvent.setTripScheduledDayOfWeek(weekDay);
        initialEvent.setTripScheduledStartPartOfDay(getPartOfTheDay(initialEvent.getTripScheduledStartTime()));
        initialEvent.setTripScheduledDayOfWeek(weekDay);

        initialEvent.setTripEventType("START");
        initialEvent.setTripEventStatus("SCHEDULED");

        initialEvent.setBusStopId(startStop.getStopid());
        initialEvent.setBusStopName(startStop.getShortname());
        initialEvent.setBusStopLatitude(startStop.getLatitude());
        initialEvent.setBusStopLongitude(startStop.getLongitude());

        initialEvent.setScheduledArrivalTime(initialEvent.getTripScheduledStartTime());

        return initialEvent;
    }

    private String getPartOfTheDay(String time) {
        String hourStr = time.split(":")[0];
        int hour = Integer.parseInt(hourStr.trim());
        if (0 <= hour && hour <= 2) {
            return "EN";
        } else if (3 <= hour && hour <= 5) {
            return "LN";
        } else if (6 <= hour && hour <= 8) {
            return "EM";
        } else if (9 <= hour && hour <= 11) {
            return "LM";
        } else if (12 <= hour && hour <= 14) {
            return "EA";
        } else if (15 <= hour && hour <= 17) {
            return "LA";
        } else if (18 <= hour && hour <= 20) {
            return "EE";
        } else if (21 <= hour && hour <= 23) {
            return "LE";
        } else {
            return null;
        }
    }

    private String createTripId(DayOfWeek weekDay, RouteResponse routeResponse, RouteType routeType, int routeTypeId, int tripCounter, StopInfo startStop) {
        StringBuilder tripId = new StringBuilder();
        tripId.append(routeResponse.getRoute()).append("-")
                .append(routeTypeId).append("-")
                .append(tripPatternFormatter.format(Date.from(Instant.now()))).append("-")
                .append(tripCounter);
        return tripId.toString();
    }

    private String createDummyTripId(DayOfWeek weekDay, RouteResponse routeResponse, RouteType routeType, int routeTypeId, int tripCounter, StopInfo startStop) {
        StringBuilder tripId = new StringBuilder();
        tripId.append(routeResponse.getRoute()).append("-")
                .append(routeTypeId).append("-")
                .append(weekDay).append("-")
                .append(tripCounter);
        return tripId.toString();
    }

    private String createTripEventId(String tripId, int stopSequence) {
        StringBuilder tripEventId = new StringBuilder(tripId);
        tripEventId.append("-").append(stopSequence);
        return tripEventId.toString();
    }

    private void findRouteSchedules(String routeId, RouteType routeType) {
        Map<String, TimeTableResponse> destinationFilteredTimeTables = getWeeklyTimeTablesDestinationFiltered(routeId, routeType);
        for (DayOfWeek day : DayOfWeek.values()) {
            Map<String, TimeTableResponse> dayFilteredTimeTablesForStops = getWeeklyTimeTablesDayFiltered(day, destinationFilteredTimeTables);
            // List<Integer> probableScheduleSizes = findMatchingScheduleBlockSize(routeId, routeType, dayFilteredTimeTablesForStops);
            for (StopInfo stopInfo : routeType.getStops()) {
                TimeTableResponse stopSchedule = dayFilteredTimeTablesForStops.get(stopInfo.getStopid());
                for (TimeTableInfo timeTableInfo : stopSchedule.getResults()) {
                    if (Objects.isNull(stopInfo.getSchedules().get(day))) {
                        stopInfo.getSchedules().put(day, new PriorityQueue(timeTableInfo.getDepartures()));
                    } else {
                        stopInfo.getSchedules().get(day).addAll(timeTableInfo.getDepartures());
                    }
                }
                /*if (probableScheduleSizes.isEmpty() && !stopSchedule.getResults().isEmpty()) {
                    throw new NoUniqueScheduleSize("No Unique Schedule Sizes found. RouteId:" + routeId);
                }*/
            }
        }
    }

    private List<Integer> findMatchingScheduleBlockSize(String routeId, RouteType routeType, Map<String, TimeTableResponse> dayFilteredTimeTablesForStops) {
        List<Integer> probableSizes = null;
        for (StopInfo stopInfo : routeType.getStops()) {
            TimeTableResponse stopSchedule = dayFilteredTimeTablesForStops.get(stopInfo.getStopid());
            if (Objects.nonNull(stopSchedule)) {
                List<Integer> currentSizes = stopSchedule.getResults().stream()
                        .map(timeTableInfo -> timeTableInfo.getDepartures().size())
                        .collect(Collectors.toList());
                probableSizes = compareAndUpdateProbableSizes(probableSizes, currentSizes);
                if (probableSizes.size() == 1) break;
            } else {
                throw new NoTimeTableResponse("No TimeTableResponse Information received from server. RouteId:" + routeId + " StopId:" + stopInfo.getStopid());
            }
        }
        return probableSizes;
    }

    private List<Integer> compareAndUpdateProbableSizes(List<Integer> probableSizes, List<Integer> currentSizes) {
        if (Objects.isNull(probableSizes)) {
            probableSizes = currentSizes;
        } else {
            List<Integer> removeTheseSizes = new ArrayList<>();
            for (Integer propSize : probableSizes) {
                if (!currentSizes.contains(propSize)) removeTheseSizes.add(propSize);
            }
            if (!removeTheseSizes.isEmpty()) {
                for (Iterator<Integer> remItr = removeTheseSizes.listIterator(); remItr.hasNext(); ) {
                    Integer remItem = remItr.next();
                    for (Iterator<Integer> probItr = probableSizes.listIterator(); probItr.hasNext(); ) {
                        if (probItr.next().equals(remItem)) {
                            probItr.remove();
                            remItr.remove();
                            break;
                        }
                    }
                }
            }
        }

        return probableSizes;
    }

    private Map<String, TimeTableResponse> getWeeklyTimeTablesDestinationFiltered(String routeId, RouteType routeType) {
        Map<String, TimeTableResponse> weeklyTimeTablesDestinationFiltered = routeType.getStops()
                .parallelStream()
                .map(stopInfo -> scrapperClient.getWeeklyTimeTableInformation(routeId, stopInfo.getStopid()))
                .collect(Collectors.toMap(TimeTableResponse::getStopid, timeTableResponse -> timeTableResponse));
        weeklyTimeTablesDestinationFiltered.values()
                .parallelStream()
                .forEach(timeTableResponse -> timeTableResponse.setResults(
                        timeTableResponse.getResults()
                                .stream()
                                .filter(timeTableInfo -> isMatchDestination(timeTableInfo.getDestination(), routeType.getDestination()))
                                .collect(Collectors.toList())
                        )
                );

        return weeklyTimeTablesDestinationFiltered;
    }

    private boolean isMatchDestination(String timeTableDestination, String routeDestination) {
        boolean isMatching = false;
        if (!timeTableDestination.equalsIgnoreCase(routeDestination)) {
            if (timeTableDestination.toUpperCase().contains(routeDestination.toUpperCase())) isMatching = true;
            if (routeDestination.toUpperCase().contains(timeTableDestination.toUpperCase())) isMatching = true;
            String firstWordTimeTableDest = timeTableDestination.split(" ")[0];
            String firstWordRouteDest = routeDestination.split(" ")[0];
            if (firstWordTimeTableDest.equalsIgnoreCase(firstWordRouteDest)) isMatching = true;
        } else {
            isMatching = true;
        }
        return isMatching;
    }

    private Map<String, TimeTableResponse> getWeeklyTimeTablesDayFiltered(DayOfWeek day, Map<String, TimeTableResponse> destinationFilteredTimeTables) {
        Map<String, TimeTableResponse> newDayFilteredTimeTables = new HashMap<>();
        for (Map.Entry<String, TimeTableResponse> entry : destinationFilteredTimeTables.entrySet()) {
            TimeTableResponse cloneOfTimeTable = modelMapper.map(entry.getValue(), TimeTableResponse.class);
            cloneOfTimeTable.setResults(cloneOfTimeTable.getResults().stream().filter(timeTableInfo -> isMatchingDay(day, timeTableInfo.getStartdayofweek(), timeTableInfo.getEnddayofweek())).collect(Collectors.toList()));
            newDayFilteredTimeTables.put(entry.getKey(), cloneOfTimeTable);
        }
        return newDayFilteredTimeTables;
    }

    private boolean isMatchingDay(DayOfWeek day, String startOfWeek, String endOfTheWeek) {
        DayOfWeek startWeekDay = getDayOfTheWeek(startOfWeek);
        DayOfWeek endWeekDay = getDayOfTheWeek(endOfTheWeek);
        return day.getValue() >= startWeekDay.getValue() && day.getValue() <= endWeekDay.getValue();
    }

    private DayOfWeek getDayOfTheWeek(String dayOfWeek) {
        DayOfWeek matchedDay = null;
        for (DayOfWeek day : DayOfWeek.values()) {
            if (day.toString().equalsIgnoreCase(dayOfWeek)) {
                matchedDay = day;
                break;
            }
        }
        return matchedDay;
    }
}
