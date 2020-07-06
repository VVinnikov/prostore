package ru.ibs.dtm.query.execution.plugin.adqm.configuration.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("env")
public class QueryEnrichmentProperties {
    private String defaultDatamart;
    private String environment;

    public String getDefaultDatamart() {
        return defaultDatamart;
    }

    public void setDefaultDatamart(String defaultDatamart) {
        this.defaultDatamart = defaultDatamart;
    }

    public String getEnvironment() {
        return environment;
    }

    public void setName(String name) {
        this.environment = name;
    }
}
