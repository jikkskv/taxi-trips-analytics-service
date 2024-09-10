package com.xyztaxicompany.analytics.trips.repo;

import com.xyztaxicompany.tsdb.AggregateOperationEnum;
import com.xyztaxicompany.tsdb.DBTimeUnit;

import java.time.LocalDateTime;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;

public interface DataFetchService<T> {
    List getAllFlattenedData(LocalDateTime startDate, LocalDateTime endDate, DBTimeUnit timeUnit);

    List<T> getAllFlattenedDataWithFilterCondition(LocalDateTime startDate, LocalDateTime endDate, DBTimeUnit timeUnit, Predicate<T> filterFunction);

    Number getAllFlattenedDataWithFilterAndAggregate(LocalDateTime startDate, LocalDateTime endDate, DBTimeUnit timeUnit, Predicate<T> filterFunction, Collector<T, ?, ? extends Number> aggregateFunction);

    Object getAllFlattenedDataWithAggregate(LocalDateTime startDate, LocalDateTime endDate, DBTimeUnit timeUnit, Predicate<T> filterFunction, AggregateOperationEnum aggregateOperationEnum, Function<T, ?> groupByFunction);

    Object getAllFlattenedDataWithGroupBy(LocalDateTime startDate, LocalDateTime endDate, DBTimeUnit timeUnit, Predicate<T> filterFunction, AggregateOperationEnum aggregateOperationEnum, Function<T, ?> groupByFunction, Collector<T, ?, Double> aggregateFunction);
}
