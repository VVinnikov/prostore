package io.arenadata.dtm.query.execution.plugin.adg.base.factory;

import io.arenadata.dtm.query.execution.plugin.adg.base.dto.AdgHelperTableNames;

public interface AdgHelperTableNamesFactory {
    AdgHelperTableNames create(String envName, String datamartMnemonic, String tableName);

    String getTablePrefix(String envName, String datamartMnemonic);
}
