package io.arenadata.dtm.query.execution.core.service.ddl.impl;

import io.arenadata.dtm.common.post.PostSqlActionType;
import io.arenadata.dtm.query.execution.core.service.metadata.InformationSchemaService;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.PostExecutor;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class UpdateInfoSchemaDdlPostExecutor implements PostExecutor<DdlRequestContext> {

    private final InformationSchemaService informationSchemaService;

    @Autowired
    public UpdateInfoSchemaDdlPostExecutor(InformationSchemaService informationSchemaService) {
        this.informationSchemaService = informationSchemaService;
    }

    @Override
    public Future<Void> execute(DdlRequestContext context) {
        return informationSchemaService.update(context.getQuery());
    }

    @Override
    public PostSqlActionType getPostActionType() {
        return PostSqlActionType.UPDATE_INFORMATION_SCHEMA;
    }
}
