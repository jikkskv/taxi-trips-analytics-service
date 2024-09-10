package com.xyztaxicompany.analytics.trips.repo;

import com.xyztaxicompany.tsdb.AggregateOperationEnum;
import com.xyztaxicompany.tsdb.DBTimeUnit;
import com.xyztaxicompany.tsdb.TimeSeriesDB;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;

@Service
public class TimeSeriesDataFetchService<T> implements DataFetchService<T> {

    private TimeSeriesDB<T> timeSeriesDB;

    public TimeSeriesDataFetchService() {
        timeSeriesDB = new TimeSeriesDB<>();
    }

    public void pushData(List<T> list, Function<T, LocalDateTime> function) {
        timeSeriesDB.pushData(list, function);
    }

    public void pushData(T t, Function<T, LocalDateTime> function) {
        timeSeriesDB.pushData(t, function);
    }

    @Override
    public List<T> getAllFlattenedData(LocalDateTime startDate, LocalDateTime endDate, DBTimeUnit timeUnit) {
        return timeSeriesDB.getAllFlattenedData(startDate, endDate, timeUnit);
    }

    @Override
    public List<T> getAllFlattenedDataWithFilterCondition(LocalDateTime startDate, LocalDateTime endDate, DBTimeUnit timeUnit, Predicate<T> filterFunction) {
        return timeSeriesDB.getAllFlattenedDataWithFilterCondition(startDate, endDate, timeUnit, filterFunction);
    }

    @Override
    public Number getAllFlattenedDataWithFilterAndAggregate(LocalDateTime startDate, LocalDateTime endDate, DBTimeUnit timeUnit, Predicate<T> filterFunction, Collector<T, ?, ? extends Number> aggregateFunction) {
        return timeSeriesDB.getAllFlattenedDataWithFilterAndAggregate(startDate, endDate, timeUnit, filterFunction, Collectors.counting());
    }

    @Override
    public Object getAllFlattenedDataWithAggregate(LocalDateTime startDate, LocalDateTime endDate, DBTimeUnit timeUnit, Predicate<T> filterFunction, AggregateOperationEnum aggregateOperationEnum, Function<T, ?> groupByFunction) {
        return timeSeriesDB.getAllFlattenedDataWithAggregate(startDate, endDate, timeUnit, filterFunction, aggregateOperationEnum, groupByFunction);
    }

    @Override
    public Object getAllFlattenedDataWithGroupBy(LocalDateTime startDate, LocalDateTime endDate, DBTimeUnit timeUnit, Predicate<T> filterFunction, AggregateOperationEnum aggregateOperationEnum, Function<T, ?> groupByFunction, Collector<T, ?, Double> aggregateFunction) {
        return timeSeriesDB.getAllFlattenedDataWithGroupBy(startDate, endDate, timeUnit, filterFunction, aggregateOperationEnum, groupByFunction, aggregateFunction);
    }
}
