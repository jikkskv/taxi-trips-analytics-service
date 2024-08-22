package com.xyztaxicompany.analytics.trips.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import static org.junit.jupiter.api.Assertions.*;

class ValidationUtilsTest {

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    @Test
    void testGetValidatedLocalDate_validDate() {
        String dateStr = "2024-08-22";
        LocalDate expectedDate = LocalDate.of(2024, 8, 22);
        LocalDate actualDate = ValidationUtils.getValidatedLocalDate(dateStr, formatter);
        assertEquals(expectedDate, actualDate);
    }

    @Test
    void testGetValidatedLocalDate_invalidDate() {
        String dateStr = "invalid-date";
        Executable executable = () -> ValidationUtils.getValidatedLocalDate(dateStr, formatter);
        assertThrows(DateTimeParseException.class, executable);
    }

    @Test
    void testValidateLatitude_validLatitude() {
        assertTrue(ValidationUtils.validateLatitude(45.0));
    }

    @Test
    void testValidateLatitude_invalidLatitude_upperLimit() {
        assertFalse(ValidationUtils.validateLatitude(95.0));
    }

    @Test
    void testValidateLatitude_invalidLatitude_lowerLimit() {
        assertFalse(ValidationUtils.validateLatitude(-95.0));
    }

    @Test
    void testValidateLongitude_validLongitude() {
        assertTrue(ValidationUtils.validateLongitude(120.0));
    }

    @Test
    void testValidateLongitude_invalidLongitude_upperLimit() {
        assertFalse(ValidationUtils.validateLongitude(190.0));
    }

    @Test
    void testValidateLongitude_invalidLongitude_lowerLimit() {
        assertFalse(ValidationUtils.validateLongitude(-190.0));
    }
}