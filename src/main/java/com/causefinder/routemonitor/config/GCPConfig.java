package com.causefinder.routemonitor.config;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

@Slf4j
@Configuration
public class GCPConfig {

    @Value("${gcp.tripdb.projectid}")
    private String projectId;
    @Value("${gcp.tripdb.clientid}")
    private String clientId;
    @Value("${gcp.tripdb.clientemail}")
    private String clientEmail;
    @Value("${gcp.tripdb.privatekeyid}")
    private String privateKeyId;
    @Value("${gcp.tripdb.privatekey}")
    private String privateKey;

    @Bean
    public GoogleCredentials getCredentials() throws IOException {
        return ServiceAccountCredentials.fromPkcs8(clientId, clientEmail, privateKey, privateKeyId, null);
    }

    @Bean
    public BigQuery getBigQuery(GoogleCredentials credentials) {
        try {
            return BigQueryOptions.newBuilder().setProjectId(projectId).setCredentials(credentials).build().getService();
        } catch (Exception ex) {
            System.out.println(ex.getStackTrace());
            log.error("Error in creating getBigQuery:", ex);
            return null;
        }
    }
}
