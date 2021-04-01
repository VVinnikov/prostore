package io.arenadata.dtm.query.execution.core.ddl.service.impl;

import io.arenadata.dtm.common.post.PostSqlActionType;
import io.arenadata.dtm.query.execution.core.metadata.service.InformationSchemaService;
import io.arenadata.dtm.query.execution.core.ddl.dto.DdlRequestContext;
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
        return informationSchemaService.update(context.getEntity(), context.getDatamartName(), context.getSqlNode().getKind());
    }

    @Override
    public PostSqlActionType getPostActionType() {
        return PostSqlActionType.UPDATE_INFORMATION_SCHEMA;
    }
}
