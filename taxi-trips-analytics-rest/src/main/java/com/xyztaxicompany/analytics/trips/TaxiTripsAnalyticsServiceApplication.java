package com.xyztaxicompany.analytics.trips;

import com.xyztaxicompany.analytics.trips.loader.ParquetDataLoader;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class TaxiTripsAnalyticsServiceApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(TaxiTripsAnalyticsServiceApplication.class, args);
        context.getBean(ParquetDataLoader.class).loadFile();
    }
}