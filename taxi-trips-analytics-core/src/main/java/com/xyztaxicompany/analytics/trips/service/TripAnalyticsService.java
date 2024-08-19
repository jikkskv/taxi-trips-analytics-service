package com.xyztaxicompany.analytics.trips.service;

import java.time.LocalDate;
import java.util.Map;

public interface TripAnalyticsService {
    Map<String, Long> getTotalTrips(LocalDate startDate, LocalDate endDate);

    Double getAvgSpeedTrips(LocalDate date);

    Map<String, Double> getAvgFareHeatMap(LocalDate date);
}
