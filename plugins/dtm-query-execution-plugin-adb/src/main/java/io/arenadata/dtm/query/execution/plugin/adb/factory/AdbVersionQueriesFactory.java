package io.arenadata.dtm.query.execution.plugin.adb.factory;

public interface AdbVersionQueriesFactory {

    String createAdbVersionQuery();

    String createFdwVersionQuery();

    String createPxfVersionQuery();
}
