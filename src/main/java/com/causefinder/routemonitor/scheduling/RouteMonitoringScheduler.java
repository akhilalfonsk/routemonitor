package com.causefinder.routemonitor.scheduling;

import com.causefinder.routemonitor.service.PathFinderService;
import com.causefinder.routemonitor.soap.model.StopData;
import com.causefinder.routemonitor.soap.model.Stops;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.javatuples.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

@Component
@Slf4j
public class RouteMonitoringScheduler {
    public static final int MONITOR_ALLOTTED_TIME_PER_SYNC_SEC=15;
    public static final int PARALLEL_MONITOR_ROUTE_GROUP_SIZE = 2;
    private static String TIME_PATTERN = "yyyy-MM-dd'T'HH:mm:ss";

    @Autowired
    PathFinderService pathFinderService;
    @Value("${routemonitor.routes}")
    private List<String> monitoredRoutesIdList;

    private AtomicLong syncCounter=new AtomicLong(0);
    private Map<Pair, Map<Stops, List<StopData>>> previousRouteStatus = new HashMap<>();

    @Scheduled(initialDelay = 30000, fixedRate = MONITOR_ALLOTTED_TIME_PER_SYNC_SEC*1000)
    public void syncMonitorRouteInbound() {
        StopWatch watch = new StopWatch();
        watch.start();
        List<Pair<String, String>> currMonitoredRoutes =getRoutesToBeMonitored();
        log.info("Status monitoring of routes {} started", currMonitoredRoutes);
        currMonitoredRoutes.parallelStream().forEach(this::updateRouteStatusSaveDelta);
        watch.stop();
        log.info("Status monitoring of routes {} completed successfully.Time Taken:{}s", currMonitoredRoutes, watch.getTime(TimeUnit.SECONDS));
    }

    private List<Pair<String, String>> getRoutesToBeMonitored() {
        List<Pair<String, String>> currMonitoredRoutes = new ArrayList<>();
        int groupCount=monitoredRoutesIdList.size()/PARALLEL_MONITOR_ROUTE_GROUP_SIZE;
        if(monitoredRoutesIdList.size()%PARALLEL_MONITOR_ROUTE_GROUP_SIZE!=0){
            groupCount++;
        }
        int groupId=(int)syncCounter.getAndIncrement()%groupCount;
        int startItem=groupId*PARALLEL_MONITOR_ROUTE_GROUP_SIZE;
        int endItem=startItem+PARALLEL_MONITOR_ROUTE_GROUP_SIZE;
        if(endItem>monitoredRoutesIdList.size()){
            endItem=monitoredRoutesIdList.size();
        }
        monitoredRoutesIdList.subList(startItem,endItem).stream().forEach(route -> {
            currMonitoredRoutes.add(Pair.with(route, "I"));
            currMonitoredRoutes.add(Pair.with(route, "O"));
        });
        return currMonitoredRoutes;
    }

    private void updateRouteStatusSaveDelta(Pair<String, String> monitoredRoute) {
        String route = monitoredRoute.getValue0();
        String direction = monitoredRoute.getValue1();
        String dirStr = "I".equalsIgnoreCase(direction) ? "Inbound" : "Outbound";
        StopWatch watch = new StopWatch();
        watch.start();
        int stopsWithStatusChange=0;
        //log.info("Monitoring thread for Route:({},{}) started", route,dirStr);
        Map<Stops, List<StopData>> currentRouteStatus = pathFinderService.getCurrentRouteStatusReport(route, direction);
        if (Objects.nonNull(previousRouteStatus.get(monitoredRoute))) {
            Map<Stops, List<StopData>> deltaStatus = pathFinderService.findDeltaStatus(previousRouteStatus.get(monitoredRoute), currentRouteStatus);
            stopsWithStatusChange=deltaStatus.size();
            pathFinderService.poolDeltaStatus(deltaStatus);
        }
        previousRouteStatus.put(monitoredRoute, currentRouteStatus);
        watch.stop();
        log.info("Monitoring thread for Route:({},{}) completed. Time taken:{}s", route, dirStr,watch.getTime(TimeUnit.SECONDS));
    }

}