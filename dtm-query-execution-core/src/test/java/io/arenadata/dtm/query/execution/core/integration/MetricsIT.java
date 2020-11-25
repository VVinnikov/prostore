package io.arenadata.dtm.query.execution.core.integration;

import lombok.Cleanup;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

public class MetricsIT extends AbstractCoreDtmIntegrationTest {

    @Test
    void metricsTest() throws IOException {
        URLConnection urlConnection = new URL("http://" + getDtmCoreMetricsHostPort() +
                "/actuator/requests/").openConnection();
        @Cleanup BufferedReader reader = new BufferedReader(new InputStreamReader(urlConnection.getInputStream()));
        String line = reader.readLine();
        System.out.println(line);
    }
}
