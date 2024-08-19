package com.xyztaxicompany.analytics.trips.loader;

import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2LatLng;
import com.xyztaxicompany.analytics.trips.trips.TripInfo;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class TripInfoConvertor extends GroupConverter {

    private static final ZoneId UTC_ZONEID = ZoneId.of("UTC");

    private TripInfo.TripInfoBuilder tripInfoBuilder;
    private TripInfo tripInfo;
    private String[] schemaFields;

    public TripInfoConvertor(MessageType messageType) {
        schemaFields = messageType.getFields().stream().map(Type::getName).toArray(String[]::new);
    }

    @Override
    public Converter getConverter(int i) {
        String schemaName = schemaFields[i];
        switch (schemaName) {
            case "unique_key":
                return new PrimitiveConverter() {
                    @Override
                    public void addBinary(Binary value) {
                        tripInfoBuilder.uniqueKey(value.toStringUsingUTF8());
                    }
                };
            case "taxi_id":
                return new PrimitiveConverter() {
                    @Override
                    public void addBinary(Binary value) {
                        tripInfoBuilder.taxiId(value.toStringUsingUTF8());
                    }
                };
            case "payment_type":
                return new PrimitiveConverter() {
                    @Override
                    public void addBinary(Binary value) {
                        tripInfoBuilder.paymentType(value.toStringUsingUTF8());
                    }
                };
            case "company":
                return new PrimitiveConverter() {
                    @Override
                    public void addBinary(Binary value) {
                        tripInfoBuilder.company(value.toStringUsingUTF8());
                    }
                };
            case "pickup_location":
                return new PrimitiveConverter() {
                    @Override
                    public void addBinary(Binary value) {
                        tripInfoBuilder.pickupLocation(value.toStringUsingUTF8());
                    }
                };
            case "dropoff_location":
                return new PrimitiveConverter() {
                    @Override
                    public void addBinary(Binary value) {
                        tripInfoBuilder.dropoffLocation(value.toStringUsingUTF8());
                    }
                };
            case "trip_start_timestamp":
                return new PrimitiveConverter() {
                    @Override
                    public void addLong(long value) {
                        tripInfoBuilder.tripStartTime(LocalDateTime.ofInstant(Instant.ofEpochMilli(value / 1000), UTC_ZONEID));
                    }
                };
            case "trip_end_timestamp":
                return new PrimitiveConverter() {
                    @Override
                    public void addLong(long value) {   //TODO: data validation
                        tripInfoBuilder.tripEndTime(LocalDateTime.ofInstant(Instant.ofEpochMilli(value / 1000), UTC_ZONEID));
                    }
                };
            case "__index_level_0__":
                return new PrimitiveConverter() {
                    @Override
                    public void addLong(long value) {
                    }
                };
            case "trip_seconds":
                return new PrimitiveConverter() {
                    @Override
                    public void addDouble(double value) {
                        tripInfoBuilder.tripSeconds(value);
                    }
                };
            case "trip_miles":
                return new PrimitiveConverter() {
                    @Override
                    public void addDouble(double value) {
                        tripInfoBuilder.tripMiles(value);
                    }
                };
            case "pickup_census_tract":
                return new PrimitiveConverter() {
                    @Override
                    public void addDouble(double value) {
                        tripInfoBuilder.pickupCommunityArea(value);
                    }
                };
            case "dropoff_census_tract":
                return new PrimitiveConverter() {
                    @Override
                    public void addDouble(double value) {
                        tripInfoBuilder.dropoffCommunityArea(value);
                    }
                };
            case "pickup_community_area":
                return new PrimitiveConverter() {
                    @Override
                    public void addDouble(double value) {
                        tripInfoBuilder.pickupCommunityArea(value);
                    }
                };
            case "dropoff_community_area":
                return new PrimitiveConverter() {
                    @Override
                    public void addDouble(double value) {
                        tripInfoBuilder.dropoffCommunityArea(value);
                    }
                };
            case "fare":
                return new PrimitiveConverter() {
                    @Override
                    public void addDouble(double value) {
                        tripInfoBuilder.fare(value);
                    }
                };
            case "tips":
                return new PrimitiveConverter() {
                    @Override
                    public void addDouble(double value) {
                        tripInfoBuilder.tips(value);
                    }
                };
            case "tolls":
                return new PrimitiveConverter() {
                    @Override
                    public void addDouble(double value) {
                        tripInfoBuilder.tolls(value);
                    }
                };
            case "extras":
                return new PrimitiveConverter() {
                    @Override
                    public void addDouble(double value) {
                        tripInfoBuilder.extras(value);
                    }
                };
            case "trip_total":
                return new PrimitiveConverter() {
                    @Override
                    public void addDouble(double value) {
                        tripInfoBuilder.tripTotal(value);
                    }
                };
            case "pickup_latitude":
                return new PrimitiveConverter() {
                    @Override
                    public void addDouble(double value) {
                        tripInfoBuilder.pickupLatitude(value);
                    }
                };
            case "pickup_longitude":
                return new PrimitiveConverter() {
                    @Override
                    public void addDouble(double value) {
                        tripInfoBuilder.pickupLongitude(value);
                    }
                };
            case "dropoff_latitude":
                return new PrimitiveConverter() {
                    @Override
                    public void addDouble(double value) {
                        tripInfoBuilder.dropoffLatitude(value);
                    }
                };
            case "dropoff_longitude":
                return new PrimitiveConverter() {
                    @Override
                    public void addDouble(double value) {
                        tripInfoBuilder.dropoffLongitude(value);
                    }
                };
            default:
                return new PrimitiveConverter() {
                    @Override
                    public void addDouble(double value) {
                    }
                };
        }
    }

    @Override
    public void start() {
        tripInfoBuilder = TripInfo.builder();
    }

    @Override
    public void end() {
        this.tripInfo = tripInfoBuilder.build();
        if (tripInfo.getPickupLatitude() != 0 && tripInfo.getPickupLongitude() != 0) {
            S2LatLng latLng = S2LatLng.fromDegrees(tripInfo.getPickupLatitude(), tripInfo.getPickupLongitude());
            tripInfo.setS2CellId(S2CellId.fromLatLng(latLng));
        }
    }

    public TripInfo getCurrentRecord() {
        return tripInfo;
    }
}
