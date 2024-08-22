package com.xyztaxicompany.analytics.trips.controller;

import com.xyztaxicompany.analytics.trips.exception.BizErrorCodeEnum;
import com.xyztaxicompany.analytics.trips.service.TripAnalyticsService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@Slf4j
@ExtendWith(MockitoExtension.class)
public class TripAnalyticsControllerTest {

    @Mock
    private TripAnalyticsService tripAnalyticsService;

    @InjectMocks
    private TripAnalyticsController tripAnalyticsController;

    @Test
    public void testGetTotalTrips_success() {
        String startDateStr = "2024-01-01";
        String endDateStr = "2024-01-31";
        LocalDate startDate = LocalDate.of(2024, 1, 1);
        LocalDate endDate = LocalDate.of(2024, 1, 31);
        Map<String, Long> tripInfoMap = new HashMap<>();
        tripInfoMap.put("2024-01-01", 100L);

        when(tripAnalyticsService.getTotalTrips(startDate, endDate)).thenReturn(tripInfoMap);
        ResponseResult response = tripAnalyticsController.getTotalTrips(startDateStr, endDateStr);
        assertNotNull(response);
        assertTrue(Objects.nonNull(response.getData()));
        assertEquals(1, ((List<Map<String, Object>>) response.getData()).size());
        verify(tripAnalyticsService, times(1)).getTotalTrips(startDate, endDate);
    }

    @Test
    public void testGetTotalTrips_invalidDate() {
        String startDateStr = "invalidDate";
        String endDateStr = "2024-01-31";
        ResponseResult response = tripAnalyticsController.getTotalTrips(startDateStr, endDateStr);
        assertNotNull(response);
        assertTrue(Objects.nonNull(response.getData()));
        assertEquals(BizErrorCodeEnum.BAD_DATA, response.getErrorCode());
        verify(tripAnalyticsService, times(0)).getTotalTrips(any(), any());
    }

    @Test
    public void testGetTotalTrips_systemError() {
        String startDateStr = "2024-01-01";
        String endDateStr = "2024-01-31";
        LocalDate startDate = LocalDate.of(2024, 1, 1);
        LocalDate endDate = LocalDate.of(2024, 1, 31);
        when(tripAnalyticsService.getTotalTrips(startDate, endDate)).thenThrow(new RuntimeException("System error"));
        ResponseResult response = tripAnalyticsController.getTotalTrips(startDateStr, endDateStr);
        assertNotNull(response);
        assertTrue(Objects.nonNull(response.getData()));
        assertEquals(BizErrorCodeEnum.SYSTEM_ERROR, response.getErrorCode());
    }

    @Test
    public void testGetAvgSpeed_success() {
        String dateStr = "2024-01-01";
        LocalDate date = LocalDate.of(2024, 1, 1);
        double avgSpeed = 30.5;
        when(tripAnalyticsService.getAvgSpeedTrips(date)).thenReturn(avgSpeed);
        ResponseResult response = tripAnalyticsController.getAvgSpeed(dateStr);
        assertNotNull(response);
        assertTrue(Objects.nonNull(response.getData()));
        assertEquals(avgSpeed, ((Map<String, Object>) response.getData()).get("average_speed"));
        verify(tripAnalyticsService, times(1)).getAvgSpeedTrips(date);
    }

    @Test
    public void testGetAvgSpeed_invalidDate() {
        String dateStr = "invalidDate";
        ResponseResult response = tripAnalyticsController.getAvgSpeed(dateStr);
        assertNotNull(response);
        assertTrue(Objects.nonNull(response.getData()));
        assertEquals(BizErrorCodeEnum.BAD_DATA, response.getErrorCode());
    }

    @Test
    public void testGetAvgFareHeatMap_success() {
        String dateStr = "2024-01-01";
        LocalDate date = LocalDate.of(2024, 1, 1);
        Map<String, Double> avgFareMap = new HashMap<>();
        avgFareMap.put("s2Id1", 15.0);
        when(tripAnalyticsService.getAvgFareHeatMap(date)).thenReturn(avgFareMap);
        ResponseResult response = tripAnalyticsController.getAvgFareHeatMap(dateStr);
        assertNotNull(response);
        assertTrue(Objects.nonNull(response.getData()));
        assertEquals(1, ((List<Map<String, Object>>) response.getData()).size());
        verify(tripAnalyticsService, times(1)).getAvgFareHeatMap(date);
    }

    @Test
    public void testGetAvgFareHeatMap_invalidDate() {
        String dateStr = "invalidDate";
        ResponseResult response = tripAnalyticsController.getAvgFareHeatMap(dateStr);
        assertNotNull(response);
        assertTrue(Objects.nonNull(response.getData()));
        assertEquals(BizErrorCodeEnum.BAD_DATA, response.getErrorCode());
    }
}
