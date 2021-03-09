package io.arenadata.dtm.query.execution.plugin.adb.factory.impl;

import io.arenadata.dtm.query.execution.plugin.adb.factory.AdbVersionQueriesFactory;
import org.springframework.stereotype.Service;

@Service
public class AdbVersionQueriesFactoryImpl implements AdbVersionQueriesFactory {

    public static final String COMPONENT_NAME_COLUMN = "name";
    public static final String VERSION_COLUMN = "version";
    private static final String ADB_NAME = "'Adb cluster'";
    private static final String FDW_NAME = "'Kafka-greenplum connector writer (fdw)'";
    private static final String PXF_NAME = "'Kafka-greenplum connector reader (pxf)'";
    private static final String CONNECTORS_QUERY = "SELECT %s as %s, extversion as %s FROM pg_catalog.pg_extension WHERE extname = '%s'";

    @Override
    public String createAdbVersionQuery() {
        return String.format("SELECT %s as %s, VERSION() as %s", ADB_NAME, COMPONENT_NAME_COLUMN, VERSION_COLUMN);
    }

    @Override
    public String createFdwVersionQuery() {
        return String.format(CONNECTORS_QUERY, FDW_NAME, COMPONENT_NAME_COLUMN, VERSION_COLUMN, "kadb_fdw");
    }

    @Override
    public String createPxfVersionQuery() {
        return String.format(CONNECTORS_QUERY, PXF_NAME, COMPONENT_NAME_COLUMN, VERSION_COLUMN, "pxf");
    }
}
