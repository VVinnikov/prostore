package io.arenadata.dtm.query.execution.plugin.adg.factory.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.plugin.adg.dto.AdgTables;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.schema.AdgSpace;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.schema.Space;
import io.arenadata.dtm.query.execution.plugin.adg.utils.AdgUtils;
import io.arenadata.dtm.query.execution.plugin.api.factory.CreateTableQueriesFactory;
import io.arenadata.dtm.query.execution.plugin.api.factory.TableEntitiesFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.function.Function;

import static io.arenadata.dtm.query.execution.plugin.adg.constants.ColumnFields.*;

@Service("adgCreateTableQueriesFactory")
public class AdgCreateTableQueriesFactory implements CreateTableQueriesFactory<AdgTables<AdgSpace>> {
    private final TableEntitiesFactory<AdgTables<Space>> tableEntitiesFactory;

    @Autowired
    public AdgCreateTableQueriesFactory(TableEntitiesFactory<AdgTables<Space>> tableEntitiesFactory) {
        this.tableEntitiesFactory = tableEntitiesFactory;
    }

    @Override
    public AdgTables<AdgSpace> create(Entity entity, String envName) {
        AdgTables<Space> tableEntities = tableEntitiesFactory.create(entity, envName);
        Function<String, String> getName = postfix ->
                AdgUtils.getSpaceName(envName, entity.getSchema(), entity.getName(),
                        postfix);
        return new AdgTables<>(
                new AdgSpace(getName.apply(ACTUAL_POSTFIX), tableEntities.getActual()),
                new AdgSpace(getName.apply(HISTORY_POSTFIX), tableEntities.getHistory()),
                new AdgSpace(getName.apply(STAGING_POSTFIX), tableEntities.getStaging())
        );
    }
}
