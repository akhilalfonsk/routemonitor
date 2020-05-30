package com.causefinder.routemonitor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class ScheduleCreatorApplication {

    public static void main(String[] args) {
        SpringApplication.run(ScheduleCreatorApplication.class, args);
    }

}
