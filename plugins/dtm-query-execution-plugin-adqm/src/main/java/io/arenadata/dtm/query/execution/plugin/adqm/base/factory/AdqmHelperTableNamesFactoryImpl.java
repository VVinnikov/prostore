package io.arenadata.dtm.query.execution.plugin.adqm.base.factory;

import io.arenadata.dtm.query.execution.plugin.adqm.base.dto.metadata.AdqmHelperTableNames;
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
