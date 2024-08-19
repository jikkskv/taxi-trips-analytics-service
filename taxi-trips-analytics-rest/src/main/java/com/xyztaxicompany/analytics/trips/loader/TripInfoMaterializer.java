package com.xyztaxicompany.analytics.trips.loader;

import com.xyztaxicompany.analytics.trips.trips.TripInfo;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

public class TripInfoMaterializer extends RecordMaterializer<TripInfo> {

    private TripInfoConvertor tripInfoConvertor;

    public TripInfoMaterializer(MessageType messageType) {
        this.tripInfoConvertor = new TripInfoConvertor(messageType);
    }

    @Override
    public TripInfo getCurrentRecord() {
        return tripInfoConvertor.getCurrentRecord();
    }

    @Override
    public GroupConverter getRootConverter() {
        return tripInfoConvertor;
    }
}
