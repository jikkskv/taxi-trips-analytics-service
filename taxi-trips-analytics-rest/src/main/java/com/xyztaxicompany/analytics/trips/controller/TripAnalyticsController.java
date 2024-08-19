package com.xyztaxicompany.analytics.trips.controller;

import com.xyztaxicompany.analytics.trips.service.TripAnalyticsService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;

@RestController
@Slf4j
public class TripAnalyticsController {

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    @Autowired
    private TripAnalyticsService tripAnalyticsService;

    @GetMapping("/total_trips")
    public ResponseEntity<Map> getTotalTrips(@RequestParam(value = "start") String startDateStr, @RequestParam(value = "end") String endDateStr) {
        LocalDate startDate = LocalDate.parse(startDateStr, formatter);
        LocalDate endDate = LocalDate.parse(endDateStr, formatter);
        Map<String, Long> tripInfoMap = tripAnalyticsService.getTotalTrips(startDate, endDate);
        return new ResponseEntity<>(tripInfoMap, HttpStatus.OK);
    }

    @GetMapping("/average_speed_24hrs")
    public ResponseEntity<Double> getAvgSpeed(@RequestParam(value = "date") String dateStr) {
        LocalDate date = LocalDate.parse(dateStr, formatter);
        Double avgSpeed = tripAnalyticsService.getAvgSpeedTrips(date);
        return new ResponseEntity<>(avgSpeed, HttpStatus.OK);
    }

    @GetMapping("/average_fare_heatmap")
    public ResponseEntity<Map> getAvgFareHeatMap(@RequestParam(value = "date") String dateStr) {
        LocalDate date = LocalDate.parse(dateStr, formatter);
        Map<String, Double> avgSpeed = tripAnalyticsService.getAvgFareHeatMap(date);
        return new ResponseEntity<>(avgSpeed, HttpStatus.OK);
    }

}
