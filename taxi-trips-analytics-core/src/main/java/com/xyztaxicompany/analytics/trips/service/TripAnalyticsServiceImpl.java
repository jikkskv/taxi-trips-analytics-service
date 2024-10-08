package com.xyztaxicompany.analytics.trips.service;

import com.xyztaxicompany.analytics.trips.repo.TimeSeriesDataFetchService;
import com.xyztaxicompany.analytics.trips.trips.TripInfo;
import com.xyztaxicompany.tsdb.AggregateOperationEnum;
import com.xyztaxicompany.tsdb.DBTimeUnit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;

@Service
public class TripAnalyticsServiceImpl implements TripAnalyticsService {

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private static final double MILES_TO_KM_CONVERSION = 1.60934 * 3600;

    private static final NumberFormat DOUBLE_FORMAT = new DecimalFormat("#0.00");

    @Value("${s2cellId.level:16}")
    public int S2_LEVEL;

    @Autowired
    private TimeSeriesDataFetchService<TripInfo> timeSeriesDataFetchService;

    @Override
    public Map<String, Long> getTotalTrips(LocalDate startDate, LocalDate endDate) {
        if (Objects.isNull(endDate) || Objects.isNull(startDate) || endDate.compareTo(startDate) < 0) {
            return Collections.emptyMap();
        }
        final Predicate<TripInfo> filterFunction = (t) -> true;
        final Function<TripInfo, String> groupByFunction = (t) -> t.getTripStartTime().format(DATE_TIME_FORMATTER);
        Map<String, Long> tripsData = (Map<String, Long>) timeSeriesDataFetchService.getAllFlattenedDataWithAggregate(startDate.atStartOfDay(), endDate.atStartOfDay(), DBTimeUnit.DAY, filterFunction, AggregateOperationEnum.COUNT, groupByFunction);
        return new TreeMap<>(tripsData);
    }

    @Override
    public Double getAvgSpeedTrips(LocalDate date) {
        if (Objects.isNull(date)) {
            return 0D;
        }
        LocalDate pastDate = date.minusDays(1);
        final Predicate<TripInfo> filterFunction = (t) -> t.getTripSeconds() > 0 && t.getTripMiles() > 0;
        final Function<TripInfo, Double> aggregateFunction = (t) -> t.getTripMiles() / t.getTripSeconds();
        Double milesPerSec = (Double) timeSeriesDataFetchService.getAllFlattenedDataWithAggregate(pastDate.atStartOfDay(), pastDate.atStartOfDay(), DBTimeUnit.DAY, filterFunction, AggregateOperationEnum.AVG, aggregateFunction);
        return Double.parseDouble(DOUBLE_FORMAT.format(MILES_TO_KM_CONVERSION * milesPerSec));
    }

    @Override
    public Map<String, Double> getAvgFareHeatMap(LocalDate date) {
        if (Objects.isNull(date)) {
            return Collections.emptyMap();
        }
        LocalDate startDate = date.minusDays(2);
        LocalDate past24HrDate = date.minusDays(1);
        final Predicate<TripInfo> filterFunction = (t) -> Objects.nonNull(t.getS2CellId()) && t.getTripEndTime().isAfter(past24HrDate.atStartOfDay()) && t.getTripEndTime().isBefore(past24HrDate.atTime(LocalTime.MAX));
        final Function<TripInfo, String> groupByFunction = (t) -> t.getS2CellId().parent(S2_LEVEL).toToken();
        final Collector<TripInfo, ?, Double> aggregateFunction = Collectors.averagingDouble(TripInfo::getFare);
        Map<String, Double> avgHeatMap = (Map<String, Double>) timeSeriesDataFetchService.getAllFlattenedDataWithGroupBy(startDate.atStartOfDay(), date.atStartOfDay(), DBTimeUnit.DAY, filterFunction, AggregateOperationEnum.GROUP_BY, groupByFunction, aggregateFunction);
        avgHeatMap.entrySet().forEach(e -> e.setValue(Double.parseDouble(DOUBLE_FORMAT.format(e.getValue()))));
        return new TreeMap<>(avgHeatMap);
    }
}
