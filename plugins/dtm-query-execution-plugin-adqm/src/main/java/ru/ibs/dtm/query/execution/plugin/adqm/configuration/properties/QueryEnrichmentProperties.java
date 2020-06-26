package ru.ibs.dtm.query.execution.plugin.adqm.configuration.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("enrichment")
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

    public void setEnvironment(String environment) {
        this.environment = environment;
    }
}
