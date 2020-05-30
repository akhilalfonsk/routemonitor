package com.causefinder.routemonitor.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.service.Contact;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import java.util.ArrayList;

@Configuration
@EnableSwagger2
public class SwaggerConfig {

    @Bean
    public Docket scheduleCreatorApi() {
        return new Docket(DocumentationType.SWAGGER_2)
                .select()
                .apis(RequestHandlerSelectors.basePackage("com.causefinder.routemonitor.controller"))
                .build().apiInfo(scheduleCreatorApiDocumentation());
    }

    private ApiInfo scheduleCreatorApiDocumentation() {
        return new ApiInfo("ScheduleCreator Backend API",
                "ScheduleCreator Backend API",
                "1.0-beta",
                "Terms and Conditions **",
                new Contact("Akhil Alfons K", "http://akhilizer.com", "akhil@akhilizer.com"),
                "MIT License",
                "MIT License",
                new ArrayList<>());
    }

}
