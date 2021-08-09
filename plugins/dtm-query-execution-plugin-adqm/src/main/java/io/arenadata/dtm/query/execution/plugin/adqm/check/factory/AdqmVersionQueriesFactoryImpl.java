package io.arenadata.dtm.query.execution.plugin.adqm.check.factory;

import org.springframework.stereotype.Service;

@Service
public class AdqmVersionQueriesFactoryImpl implements AdqmVersionQueriesFactory {

    public static final String COMPONENT_NAME_COLUMN = "name";
    public static final String VERSION_COLUMN = "version";
    private static final String ADQM_NAME = "'adqm cluster'";

    @Override
    public String createAdqmVersionQuery() {
        return String.format("SELECT %s as %s, version() as %s", ADQM_NAME, COMPONENT_NAME_COLUMN, VERSION_COLUMN);
    }
}
