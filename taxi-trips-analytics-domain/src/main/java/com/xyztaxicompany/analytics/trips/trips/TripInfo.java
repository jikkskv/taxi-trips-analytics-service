package com.xyztaxicompany.analytics.trips.trips;

import com.google.common.geometry.S2CellId;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
public class TripInfo {
    private String uniqueKey;
    private String taxiId;
    private LocalDateTime tripStartTime;
    private LocalDateTime tripEndTime;
    private double tripSeconds;
    private double tripMiles;
    private double pickupCommunityArea;
    private double dropoffCommunityArea;
    private double fare;
    private double tips;
    private double tolls;
    private double extras;
    private double tripTotal;
    private String paymentType;
    private String company;
    private double pickupLatitude;
    private double pickupLongitude;
    private String pickupLocation;  //TODO
    private double dropoffLatitude;
    private double dropoffLongitude;
    private String dropoffLocation; //TODO
    private S2CellId s2CellId;
}
