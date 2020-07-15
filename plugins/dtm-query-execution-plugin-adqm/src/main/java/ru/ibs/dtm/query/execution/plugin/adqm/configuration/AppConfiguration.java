package ru.ibs.dtm.query.execution.plugin.adqm.configuration;

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
        return environment.getProperty("env.name", String.class);
    }

    public String getDefaultDatamart() {
        return environment.getProperty("env.defaultDatamart", String.class);
    }
}
