package com.xyztaxicompany.tsdb;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AggregateFunctions {


    public static <T> Map<?, Long> applyCountFunction(Stream<T> stream, Function<T, ?> fun) {
        return stream.collect(Collectors.groupingBy(fun, Collectors.counting()));
    }

    public static <T> Number applyAggregateFunction(Stream<T> stream, Collector<T, ?, ? extends Number> aggregateFun) {
        return stream.collect(aggregateFun);
    }

    public static <T> Double applyAvgFunction(Stream<T> stream, Function<T, ?> fun) {
        return stream.collect(Collectors.averagingDouble(t -> Double.parseDouble(String.valueOf(fun.apply(t)))));
    }

    public static <T> Map<?, Double> applyGroupByAggregateFunction(Stream<T> stream, Function<T, ?> fun, Collector<T, ?, Double> aggregateFun) {
        return stream.collect(Collectors.groupingBy(fun, aggregateFun));
    }
}
