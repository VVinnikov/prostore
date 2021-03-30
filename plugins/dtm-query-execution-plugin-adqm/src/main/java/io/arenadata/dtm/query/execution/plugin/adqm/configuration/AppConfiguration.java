package io.arenadata.dtm.query.execution.plugin.adqm.configuration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
public class AppConfiguration {
    private Environment environment;

    @Autowired
    public AppConfiguration(Environment environment) {
        this.environment = environment;
    }

    public String getSystemName() {
        return environment.getProperty("core.env.name", String.class);
    }

}
