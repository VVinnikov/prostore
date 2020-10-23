package io.arenadata.dtm.query.execution.plugin.adqm.factory;

import io.arenadata.dtm.query.execution.plugin.adqm.dto.AdqmHelperTableNames;

public interface AdqmHelperTableNamesFactory {
    AdqmHelperTableNames create(String envName, String datamartMnemonic, String tableName);
}
