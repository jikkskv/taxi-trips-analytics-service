package com.xyztaxicompany.tsdb;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@SuppressWarnings("rawtypes")
public class TimeSeriesDB<T> {

    public static final ZoneOffset ZONE_OFFSET = ZoneOffset.UTC;
    private final Map<Integer, Map> dataBase;

    private final Collector<T, ?, Double> noOpCollector = Collector.of(
            () -> 0D,                  // Supplier: Initial value
            (acc, item) -> {
            },          // Accumulator: Do nothing
            (acc1, acc2) -> 0D,         // Combiner: Return 0L when combining
            acc -> 0D                   // Finisher: Always return 0L
    );

    public TimeSeriesDB() {
        dataBase = new HashMap<>();
    }

    private void addYear(int year) {
        dataBase.put(year, new ConcurrentHashMap<>(12, 12, 12));
    }

    private void addMonth(Map<Integer, Map> monthMap, int month) {
        monthMap.put(month, new ConcurrentHashMap<>(31, 31, 31));
    }

    private void addHour(Map<Integer, Map> hourMap, int hour) {
        hourMap.put(hour, new ConcurrentSkipListMap<>());
    }

    private void addDay(Map<Integer, Map> dayMap, int day) {
        dayMap.put(day, new ConcurrentHashMap<>(24, 24, 24));
    }

    private Map getYearMap(int year) {
        if (!dataBase.containsKey(year)) {
            addYear(year);
        }
        return dataBase.get(year);
    }

    private Map getMonthMap(Map<Integer, Map> monthMap, int month) {
        if (!monthMap.containsKey(month)) {
            addMonth(monthMap, month);
        }
        return monthMap.get(month);
    }

    private Map getDayMap(Map<Integer, Map> dayMap, int day) {
        if (!dayMap.containsKey(day)) {
            addDay(dayMap, day);
        }
        return dayMap.get(day);
    }

    private Map getHourMap(Map<Integer, Map> hourMap, int hour) {
        if (!hourMap.containsKey(hour)) {
            addHour(hourMap, hour);
        }
        return hourMap.get(hour);
    }

    public void pushData(List<T> list, Function<T, LocalDateTime> function) {
        list.stream().parallel().forEach(e -> pushData(e, function));
    }

    public void pushData(T t, Function<T, LocalDateTime> function) {
        LocalDateTime dateTime = function.apply(t);
        Map yearMap = getYearMap(dateTime.getYear());
        Map monthMap = getMonthMap(yearMap, dateTime.getMonthValue());
        Map dayMap = getDayMap(monthMap, dateTime.getDayOfMonth());
        Map<Long, List<T>> hourMap = getHourMap(dayMap, dateTime.getHour());
        LocalDateTime resetDateTime = dateTime.withMinute(0).withSecond(0).withNano(0);
        long diff = dateTime.toInstant(ZONE_OFFSET).toEpochMilli() - resetDateTime.toInstant(ZONE_OFFSET).toEpochMilli();
        if (!hourMap.containsKey(diff)) { //TODO: handle multithreading
            hourMap.put(diff, new CopyOnWriteArrayList<>());
        }
        hourMap.get(diff).add(t);
    }

    public List<T> getAllFlattenedData(LocalDateTime startDate, LocalDateTime endDate, DBTimeUnit timeUnit) {
        Stream<List<T>> flattenedData = getFlattenedStream(startDate, endDate, timeUnit);
        return flattenedData.parallel().flatMap(Collection::stream).toList();
    }

    public List<T> getAllFlattenedDataWithFilterCondition(LocalDateTime startDate, LocalDateTime endDate, DBTimeUnit timeUnit, Predicate<T> filterFunction) {
        Stream<List<T>> flattenedData = getFlattenedStream(startDate, endDate, timeUnit);
        return flattenedData.parallel().flatMap(Collection::stream).filter(e -> filterFunction.test(e)).toList();
    }

    public Number getAllFlattenedDataWithFilterAndAggregate(LocalDateTime startDate, LocalDateTime endDate, DBTimeUnit timeUnit, Predicate<T> filterFunction, Collector<T, ?, ? extends Number> aggregateFunction) {
        Stream<List<T>> flattenedData = getFlattenedStream(startDate, endDate, timeUnit);
        Stream<T> TList =  flattenedData.parallel().flatMap(Collection::stream).filter(e -> filterFunction.test(e));
        Object aggregatedOutput = applyAggregateFunction(TList, aggregateFunction);
        return (Number) aggregatedOutput;
    }


    public Object getAllFlattenedDataWithAggregate(LocalDateTime startDate, LocalDateTime endDate, DBTimeUnit timeUnit, Predicate<T> filterFunction, AggregateOperationEnum aggregateOperationEnum, Function<T, ?> groupByFunction) {
        Stream<List<T>> flattenedData = getFlattenedStream(startDate, endDate, timeUnit);
        Stream<T> TList = flattenedData.flatMap(e -> e.stream()).filter(e -> filterFunction.test(e));
        Object aggregatedOutput = applyAggregateFunction(TList, aggregateOperationEnum, groupByFunction, noOpCollector);
        return aggregatedOutput;
    }

    public Object getAllFlattenedDataWithGroupBy(LocalDateTime startDate, LocalDateTime endDate, DBTimeUnit timeUnit, Predicate<T> filterFunction, AggregateOperationEnum aggregateOperationEnum, Function<T, ?> groupByFunction, Collector<T, ?, Double> aggregateFunction) {
        Stream<List<T>> flattenedData = getFlattenedStream(startDate, endDate, timeUnit);
        Stream<T> TList = flattenedData.flatMap(e -> e.stream()).filter(e -> filterFunction.test(e));
        Object aggregatedOutput = applyAggregateFunction(TList, aggregateOperationEnum, groupByFunction, aggregateFunction);
        return aggregatedOutput;
    }

    private Object applyAggregateFunction(Stream<T> stream, AggregateOperationEnum aggregateOperation, Function<T, ?> groupByFunction, Collector<T, ?, Double> aggregateFunction) {
        return switch (aggregateOperation) {
            case COUNT -> AggregateFunctions.applyCountFunction(stream, groupByFunction);
            case AVG -> AggregateFunctions.applyAvgFunction(stream, groupByFunction);
            case GROUP_BY ->
                    AggregateFunctions.applyGroupByAggregateFunction(stream, groupByFunction, aggregateFunction);
        };
    }

    private Object applyAggregateFunction(Stream<T> stream, Collector<T, ?, ? extends Number> aggregateFunction) {
        return AggregateFunctions.applyAggregateFunction(stream, aggregateFunction);
    }

    private Stream<T> applyFlatStream(List<Map<Integer, Map>> dataMap, DBTimeUnit timeUnit) {
        Function<Map<Integer, Map>, Stream> flatStreamFun = (e) -> e.values().stream();
        return switch (timeUnit) {
            case HOUR -> dataMap.stream().flatMap(e1 -> flatStreamFun.apply(e1));
            case DAY ->
                    dataMap.stream().parallel().flatMap(e1 -> flatStreamFun.apply(e1)).flatMap(e2 -> flatStreamFun.apply((Map<Integer, Map>) e2));
            case MONTH ->
                    dataMap.stream().parallel().flatMap(e1 -> flatStreamFun.apply(e1)).flatMap(e2 -> flatStreamFun.apply((Map<Integer, Map>) e2)).flatMap(e3 -> flatStreamFun.apply((Map<Integer, Map>) e3));
            case YEAR ->
                    dataMap.stream().parallel().flatMap(e1 -> flatStreamFun.apply(e1)).flatMap(e2 -> flatStreamFun.apply((Map<Integer, Map>) e2)).flatMap(e3 -> flatStreamFun.apply((Map<Integer, Map>) e3)).flatMap(e4 -> flatStreamFun.apply((Map<Integer, Map>) e4));
            default -> Stream.empty();
        };
    }

    private List<Map<Integer, Map>> getDataForTimeUnit(LocalDateTime startDate, LocalDateTime endDate, DBTimeUnit timeUnit) {
        return switch (timeUnit) {
            case YEAR -> getDataMapForYear(startDate, endDate);
            case MONTH -> getDataMapForMonth(startDate, endDate);
            case DAY -> getDataMapForDay(startDate, endDate);
            case HOUR -> getDataMapForHour(startDate, endDate);
            default -> Collections.emptyList();
        };
    }

    private Stream<List<T>> getFlattenedStream(LocalDateTime startDate, LocalDateTime endDate, DBTimeUnit timeUnit) {
        if (DBTimeUnit.LOCAL_DATE_TIME.equals(timeUnit)) {
            return getDataForLocalDateTime(startDate, endDate);
        }
        List<Map<Integer, Map>> dataMap = getDataForTimeUnit(startDate, endDate, timeUnit);
        Stream<List<T>> flattenedData = (Stream<List<T>>) applyFlatStream(dataMap, timeUnit);
        return flattenedData;
    }

    private Stream<List<T>> getDataForLocalDateTime(LocalDateTime startDate, LocalDateTime endDate) {
        List dataMapForHour = getDataMapForHour(startDate, endDate);
        if (dataMapForHour.isEmpty()) {
            return Stream.empty();
        }
        long startDateTimeStampDiff = startDate.toInstant(ZONE_OFFSET.UTC).toEpochMilli() - startDate.withMinute(0).withSecond(0).withNano(0).toInstant(ZONE_OFFSET).toEpochMilli();
        long endDateTimeStampDiff = endDate.toInstant(ZONE_OFFSET.UTC).toEpochMilli() - endDate.withMinute(0).withSecond(0).withNano(0).toInstant(ZONE_OFFSET).toEpochMilli();
        Stream<List<T>> stream = Stream.<List<T>>builder().build();
        if (startDate.getHour() == endDate.getHour()) {
            stream = Stream.concat(stream, ((ConcurrentSkipListMap<Long, List<T>>) dataMapForHour.get(0)).tailMap(startDateTimeStampDiff, true).headMap(endDateTimeStampDiff, true).values().stream());
        } else {
            stream = Stream.concat(stream, ((ConcurrentSkipListMap<Long, List<T>>) dataMapForHour.get(0)).tailMap(startDateTimeStampDiff, true).values().stream());
        }
        if (dataMapForHour.size() > 2) {
            stream = Stream.concat(stream, IntStream.range(1, dataMapForHour.size() - 1).mapToObj(e -> ((ConcurrentSkipListMap<Long, List<T>>) dataMapForHour.get(e))).flatMap(e -> e.values().parallelStream()));
        }
        if (dataMapForHour.size() > 1) {
            stream = Stream.concat(stream, ((ConcurrentSkipListMap<Long, List<T>>) dataMapForHour.get(dataMapForHour.size() - 1)).headMap(endDateTimeStampDiff, true).values().stream());
        }
        return stream;
    }

    private List<Map<Integer, Map>> getDataMapForHour(LocalDateTime startDate, LocalDateTime endDate) {
        List<Map<Integer, Map>> dataMapListAtDay = getDataMapForDay(startDate, endDate);
        if (dataMapListAtDay.isEmpty()) {
            return Collections.emptyList();
        }
        List<Map<Integer, Map>> dataMapList = new ArrayList<>();
        if (startDate.getDayOfMonth() == endDate.getDayOfMonth()) {
            dataMapList.addAll(dataMapListAtDay.get(0).entrySet().stream().filter(e -> e.getKey() >= startDate.getHour() && e.getKey() <= endDate.getHour()).map(e -> (Map<Integer, Map>) e.getValue()).toList());
            return dataMapList;
        } else {
            dataMapList.addAll(dataMapListAtDay.get(0).entrySet().stream().filter(e -> e.getKey() >= startDate.getHour()).map(e -> (Map<Integer, Map>) e.getValue()).toList());
        }
        if (dataMapListAtDay.size() > 2) {
            IntStream.range(1, dataMapListAtDay.size() - 1).mapToObj(e -> dataMapListAtDay.get(e)).flatMap(e -> e.values().parallelStream()).forEach(d -> dataMapList.add((Map<Integer, Map>) d));
        }
        if (dataMapListAtDay.size() > 1) {
            dataMapList.addAll(dataMapListAtDay.get(dataMapListAtDay.size() - 1).entrySet().stream().filter(e -> e.getKey() <= endDate.getHour()).map(e -> (Map<Integer, Map>) e.getValue()).toList());
        }
        return dataMapList;
    }

    private List<Map<Integer, Map>> getDataMapForDay(LocalDateTime startDate, LocalDateTime endDate) {
        List<Map<Integer, Map>> dataMapListAtMonth = getDataMapForMonth(startDate, endDate);
        if (dataMapListAtMonth.isEmpty()) {
            return Collections.emptyList();
        }
        List<Map<Integer, Map>> dataMapList = new ArrayList<>();
        if (startDate.getMonthValue() == endDate.getMonthValue()) {
            dataMapList.addAll(dataMapListAtMonth.get(0).entrySet().stream().filter(e -> e.getKey() >= startDate.getDayOfMonth() && e.getKey() <= endDate.getDayOfMonth()).map(e -> (Map<Integer, Map>) e.getValue()).toList());
            return dataMapList;
        } else {
            dataMapList.addAll(dataMapListAtMonth.get(0).entrySet().stream().filter(e -> e.getKey() >= startDate.getDayOfMonth()).map(e -> (Map<Integer, Map>) e.getValue()).toList());
        }
        if (dataMapListAtMonth.size() > 2) {
            IntStream.range(1, dataMapListAtMonth.size() - 1).mapToObj(e -> dataMapListAtMonth.get(e)).flatMap(e -> e.values().parallelStream()).forEach(d -> dataMapList.add((Map<Integer, Map>) d));
        }
        if (dataMapListAtMonth.size() > 1) {
            dataMapList.addAll(dataMapListAtMonth.get(dataMapListAtMonth.size() - 1).entrySet().stream().filter(e -> e.getKey() <= endDate.getDayOfMonth()).map(e -> (Map<Integer, Map>) e.getValue()).toList());
        }
        return dataMapList;
    }

    private List<Map<Integer, Map>> getDataMapForMonth(LocalDateTime startDate, LocalDateTime endDate) {
        List<Map<Integer, Map>> dataMapListAtYear = getDataMapForYear(startDate, endDate);
        if (dataMapListAtYear == null || dataMapListAtYear.isEmpty()) {
            return Collections.emptyList();
        }
        List<Map<Integer, Map>> dataMapList = new ArrayList<>();
        if (startDate.getYear() == endDate.getYear()) {
            dataMapList.addAll(dataMapListAtYear.get(0).entrySet().stream().filter(e -> e.getKey() >= startDate.getMonthValue() && e.getKey() <= endDate.getMonthValue()).map(e -> (Map<Integer, Map>) e.getValue()).toList());
            return dataMapList;
        } else {
            dataMapList.addAll(dataMapListAtYear.get(0).entrySet().stream().filter(e -> e.getKey() >= startDate.getMonthValue()).map(e -> (Map<Integer, Map>) e.getValue()).toList());
        }
        if (dataMapListAtYear.size() > 2) {
            IntStream.range(1, dataMapListAtYear.size() - 2).mapToObj(e -> dataMapListAtYear.get(e)).flatMap(e -> e.values().parallelStream()).forEach(d -> dataMapList.add((Map<Integer, Map>) d));
        }
        if (dataMapListAtYear.size() > 1) {
            dataMapList.addAll(dataMapListAtYear.get(dataMapListAtYear.size() - 1).entrySet().stream().filter(e -> e.getKey() <= endDate.getMonthValue()).map(e -> (Map<Integer, Map>) e.getValue()).toList());
        }
        return dataMapList;
    }

    private List<Map<Integer, Map>> getDataMapForYear(LocalDateTime startDate, LocalDateTime endDate) {
        return IntStream.range(startDate.getYear(), endDate.getYear() + 1).mapToObj(e -> (Map<Integer, Map>) dataBase.getOrDefault(e, Collections.emptyMap())).collect(Collectors.toList());
    }
}
