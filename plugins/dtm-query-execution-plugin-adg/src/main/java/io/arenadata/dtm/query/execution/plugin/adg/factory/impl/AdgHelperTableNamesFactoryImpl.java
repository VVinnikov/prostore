package io.arenadata.dtm.query.execution.plugin.adg.factory.impl;

import io.arenadata.dtm.query.execution.plugin.adg.dto.AdgHelperTableNames;
import io.arenadata.dtm.query.execution.plugin.adg.factory.AdgHelperTableNamesFactory;
import org.springframework.stereotype.Component;

import static io.arenadata.dtm.query.execution.plugin.adg.constants.ColumnFields.*;

@Component
public class AdgHelperTableNamesFactoryImpl implements AdgHelperTableNamesFactory {

    @Override
    public AdgHelperTableNames create(String envName, String datamartMnemonic, String tableName) {
        String prefix = envName + "__" + datamartMnemonic + "__";
        return new AdgHelperTableNames(
                prefix + tableName + STAGING_POSTFIX,
                prefix + tableName + HISTORY_POSTFIX,
                prefix + tableName + ACTUAL_POSTFIX,
                prefix
        );
    }
}
