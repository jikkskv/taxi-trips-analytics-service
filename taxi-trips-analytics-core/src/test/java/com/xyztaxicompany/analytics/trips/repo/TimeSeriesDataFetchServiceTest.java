package com.xyztaxicompany.analytics.trips.repo;

import com.xyztaxicompany.analytics.trips.trips.TripInfo;
import com.xyztaxicompany.tsdb.AggregateOperationEnum;
import com.xyztaxicompany.tsdb.DBTimeUnit;
import com.xyztaxicompany.tsdb.TimeSeriesDB;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

@Slf4j
@ExtendWith(MockitoExtension.class)
class TimeSeriesDataFetchServiceTest {

    public static final Function<TripInfo, LocalDateTime> GET_TRIP_START_TIME_FUN = TripInfo::getTripStartTime;

    @Mock
    private TimeSeriesDB<TripInfo> timeSeriesDB;

    @InjectMocks
    private TimeSeriesDataFetchService<TripInfo> timeSeriesDataFetchService = new TimeSeriesDataFetchService<>();

    @Test
    void testPushData() {
        TripInfo tripInfo = TripInfo.builder().build();
        timeSeriesDataFetchService.pushData(List.of(tripInfo), GET_TRIP_START_TIME_FUN);
        timeSeriesDataFetchService.pushData(tripInfo, GET_TRIP_START_TIME_FUN);
    }

    @Test
    void getAllFlattenedData() {
        List<TripInfo> list = new ArrayList<>();
        LocalDateTime startDateTime = LocalDateTime.now();
        when(timeSeriesDataFetchService.getAllFlattenedData(eq(startDateTime), eq(startDateTime), eq(DBTimeUnit.DAY))).thenReturn(list);
        assertEquals(list, timeSeriesDataFetchService.getAllFlattenedData(startDateTime, startDateTime, DBTimeUnit.DAY));
    }

    @Test
    void getAllFlattenedDataWithAggregate() {
        List<TripInfo> list = new ArrayList<>();
        LocalDateTime startDateTime = LocalDateTime.now();
        when(timeSeriesDataFetchService.getAllFlattenedDataWithAggregate(eq(startDateTime), eq(startDateTime), eq(DBTimeUnit.DAY), any(Predicate.class), any(AggregateOperationEnum.class), any(Function.class))).thenReturn(list);
        assertEquals(list, timeSeriesDataFetchService.getAllFlattenedDataWithAggregate(startDateTime, startDateTime, DBTimeUnit.DAY, (TripInfo t) -> true, AggregateOperationEnum.AVG, GET_TRIP_START_TIME_FUN));
    }

    @Test
    void getAllFlattenedDataWithGroupBy() {
        List<TripInfo> list = new ArrayList<>();
        LocalDateTime startDateTime = LocalDateTime.now();
        when(timeSeriesDataFetchService.getAllFlattenedDataWithGroupBy(eq(startDateTime), eq(startDateTime), eq(DBTimeUnit.DAY), any(Predicate.class), any(AggregateOperationEnum.class), any(Function.class), any(Collector.class))).thenReturn(list);
        assertEquals(list, timeSeriesDataFetchService.getAllFlattenedDataWithGroupBy(startDateTime, startDateTime, DBTimeUnit.DAY, (TripInfo t) -> true, AggregateOperationEnum.AVG, GET_TRIP_START_TIME_FUN, Collectors.averagingDouble(TripInfo::getFare)));
    }
}