package io.arenadata.dtm.query.execution.plugin.adqm.factory.impl;

import io.arenadata.dtm.query.execution.plugin.adqm.dto.AdqmHelperTableNames;
import io.arenadata.dtm.query.execution.plugin.adqm.factory.AdqmHelperTableNamesFactory;
import org.springframework.stereotype.Component;

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
