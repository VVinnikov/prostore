package ru.ibs.dtm.query.execution.plugin.adg.factory.impl;

import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.execution.plugin.adg.dto.AdgHelperTableNames;
import ru.ibs.dtm.query.execution.plugin.adg.factory.AdgHelperTableNamesFactory;
import ru.ibs.dtm.query.execution.plugin.api.SystemNameRegistration;

import static ru.ibs.dtm.query.execution.plugin.adg.constants.ColumnFields.*;

@Component
public class AdgHelperTableNamesFactoryImpl implements AdgHelperTableNamesFactory, SystemNameRegistration {
    private String systemName;

    @Override
    public AdgHelperTableNames create(String datamartMnemonic, String tableName) {
        String prefix = systemName + "__" + datamartMnemonic + "__";
        return new AdgHelperTableNames(
                prefix + tableName + STAGING_POSTFIX,
                prefix + tableName + HISTORY_POSTFIX,
                prefix + tableName + ACTUAL_POSTFIX
        );
    }

    @Override
    public void register(String systemName) {
        this.systemName = systemName;
    }
}
