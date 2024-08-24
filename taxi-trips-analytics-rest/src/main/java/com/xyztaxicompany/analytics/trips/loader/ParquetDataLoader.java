package com.xyztaxicompany.analytics.trips.loader;

import com.xyztaxicompany.analytics.trips.repo.TimeSeriesDataFetchService;
import com.xyztaxicompany.analytics.trips.trips.TripInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Component
@Slf4j
public class ParquetDataLoader {

    @Value("${parquet.file.download.location}")
    private Resource parquetFileResource;

    private static final int BATCH_SIZE = 100;

    @Autowired
    private TimeSeriesDataFetchService<TripInfo> timeSeriesDataFetchService;

    public void loadFile() {
        try {
            log.info("Reading parquet file path from resource: {}", parquetFileResource);
            Path parquetFilePath = new Path(parquetFileResource.getFile().getPath());
            try (
                    ParquetReader<TripInfo> reader = AvroParquetReader.builder(new TripInfoReadSupport(), parquetFilePath)
                            .withConf(new Configuration())
                            .build()) {

                TripInfo tripInfo;
                List<TripInfo> tripInfos = new ArrayList<>();
                log.info("Started reading the file and inserting data to custom time series db");
                while ((tripInfo = reader.read()) != null) {
                    tripInfos.add(tripInfo);
                    if (tripInfos.size() == BATCH_SIZE) {
                        timeSeriesDataFetchService.pushData(tripInfos, TripInfo::getTripStartTime);
                        tripInfos.clear();
                    }
                }
                log.info("Completed reading the file and inserting data to custom time series db");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } catch (IOException e) {
            log.error("ParquetDataLoader: No file at download path");
        }
    }
}
