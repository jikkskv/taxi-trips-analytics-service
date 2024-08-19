package com.xyztaxicompany.analytics.trips.loader;

import com.xyztaxicompany.analytics.trips.trips.TripInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import java.util.Map;

public class TripInfoReadSupport extends ReadSupport<TripInfo> {

    @Override
    public ReadContext init(Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema) {
        //TODO: adjust or validate the schema if needed.
        return new ReadContext(fileSchema);  // Returns the schema found in the Parquet file
    }

    @Override
    public RecordMaterializer<TripInfo> prepareForRead(Configuration configuration, Map<String, String> map, MessageType messageType, ReadContext readContext) {
        return new TripInfoMaterializer(messageType);
    }
}
