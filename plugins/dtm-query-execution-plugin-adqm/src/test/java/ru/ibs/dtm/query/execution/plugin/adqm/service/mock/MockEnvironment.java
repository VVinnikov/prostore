package ru.ibs.dtm.query.execution.plugin.adqm.service.mock;

import org.springframework.core.env.AbstractEnvironment;

public class MockEnvironment extends AbstractEnvironment {
    @Override
    public <T> T getProperty(String key, Class<T> targetType) {
        if (key.equals("core.env.name")) {
            return (T) "dev";
        }
        return super.getProperty(key, targetType);
    }
}