package com.xyztaxicompany.analytics.trips.controller;

import com.xyztaxicompany.analytics.trips.exception.BizErrorCodeEnum;
import com.xyztaxicompany.analytics.trips.service.TripAnalyticsService;
import com.xyztaxicompany.analytics.trips.util.ValidationUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@Slf4j
public class TripAnalyticsController {

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    @Autowired
    private TripAnalyticsService tripAnalyticsService;

    @GetMapping("/total_trips")
    public ResponseResult getTotalTrips(@RequestParam(value = "start") String startDateStr, @RequestParam(value = "end") String endDateStr) {
        try {
            LocalDate startDate = ValidationUtils.getValidatedLocalDate(startDateStr, formatter);
            LocalDate endDate = ValidationUtils.getValidatedLocalDate(endDateStr, formatter);
            Map<String, Long> tripInfoMap = tripAnalyticsService.getTotalTrips(startDate, endDate);
            List<Map<String, Object>> output = createOutput(tripInfoMap, "date", "total_trips");
            return ResponseResult.success(output);
        } catch (DateTimeParseException ex) {
            return ResponseResult.failure(BizErrorCodeEnum.BAD_DATA);
        } catch (Exception ex) {
            return ResponseResult.failure(BizErrorCodeEnum.SYSTEM_ERROR);
        }
    }

    @GetMapping("/average_speed_24hrs")
    public ResponseResult getAvgSpeed(@RequestParam(value = "date") String dateStr) {
        try {
            LocalDate date = ValidationUtils.getValidatedLocalDate(dateStr, formatter);
            Double avgSpeed = tripAnalyticsService.getAvgSpeedTrips(date);
            Map<String, Object> singleEntryMap = new HashMap<>();
            singleEntryMap.put("average_speed", avgSpeed);
            return ResponseResult.success(singleEntryMap);
        } catch (DateTimeParseException ex) {
            return ResponseResult.failure(BizErrorCodeEnum.BAD_DATA);
        } catch (Exception ex) {
            return ResponseResult.failure(BizErrorCodeEnum.SYSTEM_ERROR);
        }
    }

    @GetMapping("/average_fare_heatmap")
    public ResponseResult getAvgFareHeatMap(@RequestParam(value = "date") String dateStr) {
        try {
            LocalDate date = ValidationUtils.getValidatedLocalDate(dateStr, formatter);
            Map<String, Double> avgSpeed = tripAnalyticsService.getAvgFareHeatMap(date);
            List<Map<String, Object>> output = createOutput(avgSpeed, "s2Id", "fare");
            return ResponseResult.success(output);
        } catch (DateTimeParseException ex) {
            return ResponseResult.failure(BizErrorCodeEnum.BAD_DATA);
        } catch (Exception ex) {
            return ResponseResult.failure(BizErrorCodeEnum.SYSTEM_ERROR);
        }
    }

    private List<Map<String, Object>> createOutput(Map<String, ?> dataMap, String inputName, String valueName) {
        return dataMap.entrySet()
                .stream()
                .map(entry -> {
                    Map<String, Object> outputMapFormat = new HashMap<>();
                    outputMapFormat.put(inputName, entry.getKey());
                    outputMapFormat.put(valueName, entry.getValue());
                    return outputMapFormat;
                })
                .collect(Collectors.toList());
    }
}
