package io.arenadata.dtm.query.execution.plugin.adqm.base.factory;

import io.arenadata.dtm.query.execution.plugin.adqm.base.dto.metadata.AdqmHelperTableNames;

public interface AdqmHelperTableNamesFactory {
    AdqmHelperTableNames create(String envName, String datamartMnemonic, String tableName);
}
