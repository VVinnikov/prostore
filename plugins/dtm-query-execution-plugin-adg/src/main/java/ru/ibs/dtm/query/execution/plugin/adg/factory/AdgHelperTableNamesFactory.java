package ru.ibs.dtm.query.execution.plugin.adg.factory;

import ru.ibs.dtm.query.execution.plugin.adg.dto.AdgHelperTableNames;

public interface AdgHelperTableNamesFactory {
    AdgHelperTableNames create(String datamartMnemonic, String tableName);
}
