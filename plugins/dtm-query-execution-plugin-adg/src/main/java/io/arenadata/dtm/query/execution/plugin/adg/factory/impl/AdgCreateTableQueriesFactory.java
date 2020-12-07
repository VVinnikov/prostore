package io.arenadata.dtm.query.execution.plugin.adg.factory.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.plugin.adg.dto.AdgTables;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.schema.*;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.factory.CreateTableQueriesFactory;
import io.arenadata.dtm.query.execution.plugin.api.factory.TableEntitiesFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static io.arenadata.dtm.query.execution.plugin.adg.constants.ColumnFields.*;

@Service("adgCreateTableQueriesFactory")
public class AdgCreateTableQueriesFactory implements CreateTableQueriesFactory<AdgTables<AdgSpace>> {
    public static final String TABLE_NAME_DELIMITER = "__";
    private final TableEntitiesFactory<AdgTables<Space>> tableEntitiesFactory;

    @Autowired
    public AdgCreateTableQueriesFactory(TableEntitiesFactory<AdgTables<Space>> tableEntitiesFactory) {
        this.tableEntitiesFactory = tableEntitiesFactory;
    }

    @Override
    public AdgTables<AdgSpace> create(DdlRequestContext context) {
        QueryRequest queryRequest = context.getRequest().getQueryRequest();
        String prefix = queryRequest.getEnvName() + TABLE_NAME_DELIMITER +
                queryRequest.getDatamartMnemonic() + TABLE_NAME_DELIMITER;
        Entity entity = context.getRequest().getEntity();
        int indexComma = entity.getName().indexOf(".");
        String table = entity.getName().substring(indexComma + 1).toLowerCase();
        AdgTables<Space> tableEntities = tableEntitiesFactory.create(context.getRequest().getEntity(),
                context.getRequest().getQueryRequest().getEnvName());

        return new AdgTables<>(
                new AdgSpace(prefix + table + ACTUAL_POSTFIX, tableEntities.getActual()),
                new AdgSpace(prefix + table + HISTORY_POSTFIX, tableEntities.getHistory()),
                new AdgSpace(prefix + table + STAGING_POSTFIX, tableEntities.getStaging())
        );
    }
}
