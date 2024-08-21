package com.xyztaxicompany.analytics.trips.util;

import lombok.extern.slf4j.Slf4j;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

@Slf4j
public class ValidationUtils {

    public static LocalDate getValidatedLocalDate(String dateStr, DateTimeFormatter formatter) {
        try {
            return LocalDate.parse(dateStr, formatter);
        } catch (DateTimeParseException ex) {
            log.error("Error occurred in parsing date: {}", dateStr);
            throw ex;
        }
    }

    public static boolean validateLatitude(double latitude) {
        return latitude >= -90 && latitude <= 90;
    }

    public static boolean validateLongitude(double longitude) {
        return longitude >= -180 && longitude <= 180;
    }
}
