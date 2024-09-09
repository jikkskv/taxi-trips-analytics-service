package com.xyztaxicompany.analytics.trips.loader;

import com.xyztaxicompany.analytics.trips.trips.TripInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@Slf4j
@ExtendWith(MockitoExtension.class)
class TripInfoConvertorTest {

    private TripInfo.TripInfoBuilder tripInfoBuilder;

    private TripInfoConvertor tripInfoConvertor;

    @BeforeEach
    void setUp() {
        // Mock the TripInfoBuilder
        tripInfoBuilder = mock(TripInfo.TripInfoBuilder.class);

        // Create a MessageType with the required fields
        MessageType messageType = Types.buildMessage()
                .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).named("unique_key"))
                .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).named("taxi_id"))
                .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).named("payment_type"))
                .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).named("company"))
                .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).named("pickup_location"))
                .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).named("dropoff_location"))
                .addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("trip_start_timestamp"))
                .addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("trip_start_timestamp"))
                .addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("__index_level_0__"))
                .addField(Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named("trip_seconds"))
                .addField(Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named("trip_miles"))
                .addField(Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named("pickup_census_tract"))
                .addField(Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named("dropoff_census_tract"))
                .addField(Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named("pickup_community_area"))
                .addField(Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named("dropoff_community_area"))
                .addField(Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named("fare"))
                .addField(Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named("tips"))
                .addField(Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named("tolls"))
                .addField(Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named("extras"))
                .addField(Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named("trip_total"))
                .addField(Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named("pickup_latitude"))
                .addField(Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named("pickup_longitude"))
                .addField(Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named("dropoff_latitude"))
                .addField(Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named("dropoff_longitude"))
                .named("TripInfo");

        tripInfoConvertor = new TripInfoConvertor(tripInfoBuilder, messageType);
    }

    @Test
    void getConverter() {
        for (int idx = 0; idx < 6; idx++) {
            PrimitiveConverter uniqueKeyConverter = (PrimitiveConverter) tripInfoConvertor.getConverter(idx);
            uniqueKeyConverter.addBinary(Binary.fromString("test_key"));
        }
        for (int idx = 6; idx < 9; idx++) {
            PrimitiveConverter uniqueKeyConverter = (PrimitiveConverter) tripInfoConvertor.getConverter(idx);
            uniqueKeyConverter.addLong(1L);
        }
        for (int idx = 9; idx < 24; idx++) {
            PrimitiveConverter uniqueKeyConverter = (PrimitiveConverter) tripInfoConvertor.getConverter(idx);
            uniqueKeyConverter.addDouble(1D);
        }
    }

    @Test
    void testGetConverterForPrimitiveTypes() {
        // Testing primitive converters
        PrimitiveConverter uniqueKeyConverter = (PrimitiveConverter) tripInfoConvertor.getConverter(0);
        uniqueKeyConverter.addBinary(Binary.fromString("test_key"));
        verify(tripInfoBuilder).uniqueKey("test_key");
    }

    @Test
    void testGetConverterForTimestamps() {
        // Testing timestamp conversion
        PrimitiveConverter timestampConverter = (PrimitiveConverter) tripInfoConvertor.getConverter(6); // Index for trip_start_timestamp
        long timestamp = Instant.now().toEpochMilli();
        timestampConverter.addLong(timestamp);
        LocalDateTime expectedTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp / 1000), ZoneId.of("UTC"));
        verify(tripInfoBuilder).tripStartTime(expectedTime);
    }

    @Test
    void testStartAndEnd() {
        tripInfoConvertor.start();
        tripInfoConvertor.end();
        TripInfo tripInfo = tripInfoConvertor.getCurrentRecord();
        assertNull(tripInfo);
        // Verify further as necessary
    }

    @Test
    void testValidation() {
        TripInfo validTripInfo = TripInfo.builder()
                .tripStartTime(LocalDateTime.now().minusHours(1))
                .tripEndTime(LocalDateTime.now())
                .tripSeconds(10.0)
                .build();
        assertFalse(tripInfoConvertor.validateTripsData(validTripInfo));

        TripInfo invalidTripInfo = TripInfo.builder()
                .tripStartTime(LocalDateTime.now())
                .tripEndTime(LocalDateTime.now().minusHours(1))
                .tripSeconds(-10.0)
                .build();
        assertTrue(tripInfoConvertor.validateTripsData(invalidTripInfo));
    }

    @Test
    void testSchemaFields() {
        String[] schemaFields = {"unique_key", "taxi_id"};
        tripInfoConvertor.setSchemaFields(schemaFields);
        // Check if the fields are set correctly
        // Depending on the implementation details, you might need additional verifications
    }
}
