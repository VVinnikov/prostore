package ru.ibs.dtm.query.execution.plugin.adqm.factory.impl;

import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.execution.plugin.adqm.dto.AdqmHelperTableNames;
import ru.ibs.dtm.query.execution.plugin.adqm.factory.AdqmHelperTableNamesFactory;

@Component
public class AdqmHelperTableNamesFactoryImpl implements AdqmHelperTableNamesFactory {

    @Override
    public AdqmHelperTableNames create(String envName, String datamartMnemonic, String tableName) {
        String schema = envName + "__" + datamartMnemonic;
        return new AdqmHelperTableNames(
            schema,
            tableName + "_actual",
            tableName + "_actual_shard"
        );
    }
}
