package com.xyztaxicompany.tsdb;


import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class TimeSeriesDBTest {

    private static TimeSeriesDB<Person> timeSeriesDB;
    private static final NumberFormat DOUBLE_FORMAT = new DecimalFormat("#0.00");

    static final List<Person> personList = List.of(
            new Person(0, "name_0", LocalDate.of(2019, 1, 12).atStartOfDay(), 12.21),
            new Person(1, "name_1", LocalDate.of(2020, 1, 12).atStartOfDay(), 15.63),
            new Person(2, "name_2", LocalDate.of(2020, 1, 15).atStartOfDay(), 25.23),
            new Person(3, "name_3", LocalDate.of(2020, 1, 21).atStartOfDay(), 45.48),
            new Person(4, "name_4", LocalDate.of(2020, 2, 1).atStartOfDay(), 75.57),
            new Person(5, "name_5", LocalDate.of(2020, 3, 1).atStartOfDay(), 15.69),
            new Person(6, "name_6", LocalDateTime.of(2020, 4, 5, 13, 30, 12), 15.84),
            new Person(7, "name_7", LocalDateTime.of(2020, 4, 14, 13, 50, 12), 18.62),
            new Person(8, "name_8", LocalDateTime.of(2020, 7, 17, 14, 50, 12), 12.17),
            new Person(9, "name_9", LocalDateTime.of(2020, 8, 24, 15, 50, 12), 19.25)
    );

    @BeforeAll
    public static void init() {
        timeSeriesDB = new TimeSeriesDB<>();
        timeSeriesDB.pushData(personList, Person::getDateOfBirth);

    }

    @Test
    void getAllFlattenedData_emptyData() {
        TimeSeriesDB<Person> timeSeriesDBTemp = new TimeSeriesDB<>();
        List<Person> outputList = timeSeriesDBTemp.getAllFlattenedData(LocalDate.of(2020, 1, 12).atStartOfDay(), LocalDate.of(2020, 1, 21).atStartOfDay(), DBTimeUnit.DAY);
        assertNotNull(outputList);
        assertTrue(outputList.isEmpty());
    }

    @Test
    void getAllFlattenedDataWithFilterCondition() {
        List<Person> outputList = timeSeriesDB.getAllFlattenedDataWithFilterCondition(LocalDate.of(2020, 1, 12).atStartOfDay(), LocalDate.of(2020, 1, 21).atStartOfDay(), DBTimeUnit.DAY, (Person p)-> p.getAge()>26);
        assertNotNull(outputList);
        assertFalse(outputList.isEmpty());
        Set<String> nameSet = outputList.stream().map(e -> e.name).collect(Collectors.toSet());
        assertEquals(1, nameSet.size());
        assertTrue(nameSet.contains("name_3"));
    }

    @Test
    void getAllFlattenedDataWithFilterAndAggregate() {
        Number count = timeSeriesDB.getAllFlattenedDataWithFilterAndAggregate(LocalDate.of(2020, 1, 12).atStartOfDay(), LocalDate.of(2020, 1, 21).atStartOfDay(), DBTimeUnit.DAY, (Person p)-> true, Collectors.counting());
        assertEquals(3L, count.longValue());
    }

    @Test
    void getAllFlattenedData_singleMonthTest() {
        List<Person> outputList = timeSeriesDB.getAllFlattenedData(LocalDate.of(2020, 1, 12).atStartOfDay(), LocalDate.of(2020, 1, 21).atStartOfDay(), DBTimeUnit.DAY);
        assertNotNull(outputList);
        assertFalse(outputList.isEmpty());
        Set<String> nameSet = outputList.stream().map(e -> e.name).collect(Collectors.toSet());
        assertEquals(3, nameSet.size());
        assertTrue(nameSet.contains("name_2"));
        assertTrue(nameSet.contains("name_3"));
    }

    @Test
    void getAllFlattenedData_multipleMonthTestIncludingEndDate() {
        List<Person> outputList = timeSeriesDB.getAllFlattenedData(LocalDate.of(2020, 1, 12).atStartOfDay(), LocalDate.of(2020, 4, 14).atStartOfDay(), DBTimeUnit.DAY);
        assertNotNull(outputList);
        assertFalse(outputList.isEmpty());
        assertEquals(7, outputList.size());
        assertTrue(outputList.stream().filter(e -> e.id == 7).anyMatch(e -> e.getDateOfBirth().getMonthValue() == 4 && e.getDateOfBirth().getDayOfMonth() == 14));
    }

    @Test
    void getAllFlattenedData_noDataForMonth() {
        List<Person> outputList = timeSeriesDB.getAllFlattenedData(LocalDate.of(2020, 6, 1).atStartOfDay(), LocalDate.of(2020, 6, 30).atStartOfDay(), DBTimeUnit.DAY);
        assertNotNull(outputList);
        assertTrue(outputList.isEmpty());
    }

    @Test
    void getAllFlattenedData_emptyDataForStartMonth() {
        List<Person> outputList = timeSeriesDB.getAllFlattenedData(LocalDate.of(2020, 5, 1).atStartOfDay(), LocalDate.of(2020, 7, 31).atStartOfDay(), DBTimeUnit.DAY);
        assertNotNull(outputList);
        assertFalse(outputList.isEmpty());
        assertEquals(1, outputList.size());
    }

    @Test
    void getAllFlattenedData_emptyDataForEndMonth() {
        List<Person> outputList = timeSeriesDB.getAllFlattenedData(LocalDate.of(2020, 4, 1).atStartOfDay(), LocalDate.of(2020, 6, 30).atStartOfDay(), DBTimeUnit.DAY);
        assertNotNull(outputList);
        assertFalse(outputList.isEmpty());
        assertEquals(2, outputList.size());
    }

    @Test
    void getAllFlattenedData_emptyDataForEndMonthInRange() {
        List<Person> outputList = timeSeriesDB.getAllFlattenedData(LocalDate.of(2020, 4, 1).atStartOfDay(), LocalDate.of(2020, 7, 31).atStartOfDay(), DBTimeUnit.DAY);
        assertNotNull(outputList);
        assertFalse(outputList.isEmpty());
        assertEquals(3, outputList.size());
    }

    @Test
    void getAllFlattenedData_dataForMonthRange() {
        List<Person> outputList = timeSeriesDB.getAllFlattenedData(LocalDate.of(2020, 1, 22).atStartOfDay(), LocalDate.of(2020, 4, 01).atStartOfDay(), DBTimeUnit.MONTH);
        assertNotNull(outputList);
        assertFalse(outputList.isEmpty());
        assertEquals(7, outputList.size());
    }

    @Test
    void getAllFlattenedData_dataForHourRange() {
        List<Person> outputList = timeSeriesDB.getAllFlattenedData(LocalDateTime.of(2020, 4, 5, 13, 30), LocalDateTime.of(2020, 7, 17, 14, 50), DBTimeUnit.HOUR);
        assertNotNull(outputList);
        assertFalse(outputList.isEmpty());
        assertEquals(3, outputList.size());
    }

    @Test
    void getAllFlattenedData_emptyDataForHourRange() {
        List<Person> outputList = timeSeriesDB.getAllFlattenedData(LocalDateTime.of(2020, 4, 5, 14, 31), LocalDateTime.of(2020, 4, 14, 12, 49), DBTimeUnit.HOUR);
        assertNotNull(outputList);
        assertTrue(outputList.isEmpty());
    }

    @Test
    void getAllFlattenedData_dataForYearRange() {
        List<Person> outputList = timeSeriesDB.getAllFlattenedData(LocalDateTime.of(2019, 4, 5, 14, 31), LocalDateTime.of(2020, 4, 14, 12, 49), DBTimeUnit.YEAR);
        assertNotNull(outputList);
        assertFalse(outputList.isEmpty());
        assertEquals(10, outputList.size());
    }

    @Test
    void getAllFlattenedData_localDateTimeDataForSameHour() {
        List<Person> outputList = timeSeriesDB.getAllFlattenedData(LocalDateTime.of(2020, 4, 5, 13, 30), LocalDateTime.of(2020, 4, 5, 13, 31), DBTimeUnit.LOCAL_DATE_TIME);
        assertNotNull(outputList);
        assertFalse(outputList.isEmpty());
        assertEquals(1, outputList.size());
    }

    @Test
    void getAllFlattenedData_localDateTimeDataForDifferentDay() {
        List<Person> outputList = timeSeriesDB.getAllFlattenedData(LocalDateTime.of(2020, 4, 5, 13, 30, 12), LocalDateTime.of(2020, 7, 17, 14, 50, 12), DBTimeUnit.LOCAL_DATE_TIME);
        assertNotNull(outputList);
        assertFalse(outputList.isEmpty());
        assertEquals(3, outputList.size());
    }

    @Test
    void getAllFlattenedData_localDateTimeDataAtSecondLevel() {
        List<Person> outputList = timeSeriesDB.getAllFlattenedData(LocalDateTime.of(2020, 4, 5, 13, 30, 11), LocalDateTime.of(2020, 4, 5, 13, 30, 12), DBTimeUnit.LOCAL_DATE_TIME);
        assertNotNull(outputList);
        assertFalse(outputList.isEmpty());
        assertEquals(1, outputList.size());
    }

    @Test
    void getAllFlattenedData_emptyDataAtSecondLevel() {
        List<Person> outputList = timeSeriesDB.getAllFlattenedData(LocalDateTime.of(2020, 4, 5, 13, 30, 13), LocalDateTime.of(2020, 4, 5, 13, 30, 23), DBTimeUnit.LOCAL_DATE_TIME);
        assertNotNull(outputList);
        assertTrue(outputList.isEmpty());
    }

    @Test
    void getAllFlattenedDataWithAggregate_testCountAtDayLevel() {
        Predicate<Person> filter = p -> true;
        Map<String, Long> personData = (Map<String, Long>) timeSeriesDB.getAllFlattenedDataWithAggregate(LocalDate.of(2020, 1, 12).atStartOfDay(), LocalDate.of(2020, 4, 14).atStartOfDay(), DBTimeUnit.DAY, filter, AggregateOperationEnum.COUNT, Person::getId);
        assertNotNull(personData);
        IntStream.range(1, 8).forEach(e -> assertTrue(personData.get(e) == 1));
    }

    @Test
    void getAllFlattenedDataWithAggregate_testCountAtMonthLevel() {
        Predicate<Person> filter = p -> true;
        Function<Person, Month> aggregatorFun = e -> e.getDateOfBirth().getMonth();
        Map<Month, Long> personData = (Map<Month, Long>) timeSeriesDB.getAllFlattenedDataWithAggregate(LocalDate.of(2020, 1, 12).atStartOfDay(), LocalDate.of(2020, 4, 13).atStartOfDay(), DBTimeUnit.DAY, filter, AggregateOperationEnum.COUNT, aggregatorFun);
        assertNotNull(personData);
        assertEquals(3L, personData.get(Month.JANUARY).longValue());
        assertEquals(1L, personData.get(Month.FEBRUARY).longValue());
        assertEquals(1L, personData.get(Month.MARCH).longValue());
        assertEquals(1L, personData.get(Month.APRIL).longValue());
    }

    @Test
    void getAllFlattenedDataWithAggregate_testAvgAtMonthLevel() {
        Predicate<Person> filter = p -> true;
        Function<Person, Double> aggregatorFun = Person::getAge;
        Double ageDouble = (Double) timeSeriesDB.getAllFlattenedDataWithAggregate(LocalDate.of(2020, 1, 12).atStartOfDay(), LocalDate.of(2020, 4, 13).atStartOfDay(), DBTimeUnit.DAY, filter, AggregateOperationEnum.AVG, aggregatorFun);
        assertEquals(0, Double.valueOf(32.24).compareTo(Double.valueOf(DOUBLE_FORMAT.format(ageDouble))));
    }

    @Test
    void getAllFlattenedDataWithGroupBy() {
        Predicate<Person> filter = p -> true;
        Function<Person, Month> groupByFun = e -> e.getDateOfBirth().getMonth();
        Collector<Person, ?, Double> aggregatorFun = Collectors.averagingDouble(Person::getAge);
        Map<Month, Double> personData = (Map<Month, Double>) timeSeriesDB.getAllFlattenedDataWithGroupBy(LocalDate.of(2020, 1, 12).atStartOfDay(), LocalDate.of(2020, 4, 15).atStartOfDay(), DBTimeUnit.DAY, filter, AggregateOperationEnum.GROUP_BY, groupByFun, aggregatorFun);
        assertNotNull(personData);
        assertEquals(0, Double.valueOf(28.78).compareTo(Double.valueOf(DOUBLE_FORMAT.format(personData.get(Month.JANUARY)))));
        assertEquals(75.57, personData.get(Month.FEBRUARY).doubleValue());
        assertEquals(15.69, personData.get(Month.MARCH).doubleValue());
        assertEquals(17.23, personData.get(Month.APRIL).doubleValue());
    }

    static class Person {
        int id;
        String name;
        double age;
        LocalDateTime dateOfBirth;

        Person(int id, String name, LocalDateTime dateOfBirth, double age) {
            this.id = id;
            this.name = name;
            this.dateOfBirth = dateOfBirth;
            this.age = age;
        }

        public int getId() {
            return id;
        }

        public LocalDateTime getDateOfBirth() {
            return dateOfBirth;
        }

        public double getAge() {
            return age;
        }
    }
}
