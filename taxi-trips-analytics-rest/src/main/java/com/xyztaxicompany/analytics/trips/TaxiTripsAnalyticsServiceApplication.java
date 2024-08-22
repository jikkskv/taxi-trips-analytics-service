package com.xyztaxicompany.analytics.trips;

import com.xyztaxicompany.analytics.trips.loader.ParquetDataLoader;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;

@SpringBootApplication
@Slf4j
public class TaxiTripsAnalyticsServiceApplication implements CommandLineRunner {

    @Autowired
    private Environment environment;

    private static String sPort;

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(TaxiTripsAnalyticsServiceApplication.class, args);
        context.getBean(ParquetDataLoader.class).loadFile();
        log.info("Starting taxi analytics at port {} ", sPort);
    }

    @Override
    public void run(String... args) throws Exception {
        sPort = environment.getProperty("server.port");
    }
}