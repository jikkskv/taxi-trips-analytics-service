package com.xyztaxicompany.tsdb;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AggregateFunctionsTest {

    @Test
    void applyCountFunction_sampleTest() {
        Map<Integer, Long> countMap = (Map<Integer, Long>) AggregateFunctions.applyCountFunction(Stream.of(1, 1, 2, 2, 2, 3), (Integer a) -> a);
        assertEquals(2, countMap.get(1));
        assertEquals(3, countMap.get(2));
        assertEquals(1, countMap.get(3));
    }

    @Test
    void applyAggregateFunction_sampleTest() {
        Number count = AggregateFunctions.applyAggregateFunction(Stream.of(1, 1, 2, 2, 2, 3), Collectors.counting());
        assertEquals(6L, count.longValue());
    }

    @Test
    void applyAvgFunction() {
        Double aDouble = AggregateFunctions.applyAvgFunction(Stream.of(1, 2, 2, 2, 3), (Integer a) -> a);
        assertEquals(2, aDouble);
    }

    @Test
    void applyGroupByAggregateFunction() {
        Map<Integer, Double> countMap = (Map<Integer, Double>) AggregateFunctions.applyGroupByAggregateFunction(Stream.of(1, 1, 2, 2, 2, 3), (Integer a) -> a, Collectors.averagingDouble(Integer::doubleValue));
        assertEquals(1.0, countMap.get(1).doubleValue());
        assertEquals(2.0, countMap.get(2).doubleValue());
        assertEquals(3.0, countMap.get(3).doubleValue());
    }
}