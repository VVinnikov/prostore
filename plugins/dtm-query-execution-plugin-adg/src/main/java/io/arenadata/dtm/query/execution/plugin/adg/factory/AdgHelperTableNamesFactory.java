package io.arenadata.dtm.query.execution.plugin.adg.factory;

import io.arenadata.dtm.query.execution.plugin.adg.dto.AdgHelperTableNames;

public interface AdgHelperTableNamesFactory {
    AdgHelperTableNames create(String envName, String datamartMnemonic, String tableName);

    String getTablePrefix(String envName, String datamartMnemonic);
}
