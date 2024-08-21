package com.xyztaxicompany.analytics.trips.service;

import com.xyztaxicompany.analytics.trips.repo.TimeSeriesDataFetchService;
import com.xyztaxicompany.analytics.trips.trips.TripInfo;
import com.xyztaxicompany.tsdb.AggregateOperationEnum;
import com.xyztaxicompany.tsdb.DBTimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

@Slf4j
@ExtendWith(MockitoExtension.class)
class TripAnalyticsServiceImplTest {

    @Mock
    private TimeSeriesDataFetchService<TripInfo> timeSeriesDataFetchService;

    @InjectMocks
    private TripAnalyticsServiceImpl tripAnalyticsService;

    @Test
    void getTotalTrips_nullStartDate() {
        assertTrue(tripAnalyticsService.getTotalTrips(null, LocalDate.now()).isEmpty());
    }

    @Test
    void getTotalTrips_nullEndDate() {
        assertTrue(tripAnalyticsService.getTotalTrips(LocalDate.now(), null).isEmpty());
    }

    @Test
    void getTotalTrips_startDateMoreThanEndData() {
        LocalDate startDate = LocalDate.now();
        assertTrue(tripAnalyticsService.getTotalTrips(startDate.plusDays(1), startDate).isEmpty());
    }

    @Test
    void getTotalTrips_validData() {
        Map<String, Long> tripsData = new HashMap<>();
        LocalDate startDate = LocalDate.now();
        when(timeSeriesDataFetchService.getAllFlattenedDataWithAggregate(any(LocalDateTime.class), any(LocalDateTime.class), eq(DBTimeUnit.DAY), any(Predicate.class), any(AggregateOperationEnum.class), any(Function.class)))
                .thenReturn(tripsData);
        assertEquals(tripsData, tripAnalyticsService.getTotalTrips(startDate, startDate.plusDays(1)));
    }

    @Test
    void getAvgSpeedTrips_nullDate() {
        assertEquals(0D, tripAnalyticsService.getAvgSpeedTrips(null));
    }

    @Test
    void getAvgSpeedTrips_validData() {
        when(timeSeriesDataFetchService.getAllFlattenedDataWithAggregate(any(LocalDateTime.class), any(LocalDateTime.class), eq(DBTimeUnit.DAY), any(Predicate.class), any(AggregateOperationEnum.class), any(Function.class)))
                .thenReturn(100D);
        assertEquals(100D * 1.60934 * 3600, tripAnalyticsService.getAvgSpeedTrips(LocalDate.now()));
    }

    @Test
    void getAvgFareHeatMap_nullDate() {
        assertTrue(tripAnalyticsService.getAvgFareHeatMap(null).isEmpty());
    }

    @Test
    void getAvgFareHeatMap_validData() {
        Map<String, Long> tripsData = new HashMap<>();
        when(timeSeriesDataFetchService.getAllFlattenedDataWithGroupBy(any(LocalDateTime.class), any(LocalDateTime.class), eq(DBTimeUnit.DAY), any(Predicate.class), any(AggregateOperationEnum.class), any(Function.class), any(Collector.class)))
                .thenReturn(tripsData);
        assertEquals(tripsData, tripAnalyticsService.getAvgFareHeatMap(LocalDate.now()));
    }
}