package com.xyztaxicompany.analytics.trips.util;

import lombok.extern.slf4j.Slf4j;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

@Slf4j
public class DateUtils {

    public static LocalDate getParsedLocalDate(String dateStr, DateTimeFormatter formatter) {
        try {
            return LocalDate.parse(dateStr, formatter);
        } catch (DateTimeParseException ex) {
            log.error("Error occurred in parsing date: {}", dateStr);
            throw ex;
        }
    }
}
