package io.arenadata.dtm.query.execution.plugin.adg.service.impl.ddl;

import io.arenadata.dtm.async.AsyncHandler;
import io.arenadata.dtm.query.calcite.core.extension.ddl.SqlCreateTable;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.OperationYaml;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.response.ResOperation;
import io.arenadata.dtm.query.execution.plugin.adg.service.AdgCartridgeClient;
import io.arenadata.dtm.query.execution.plugin.adg.service.TtCartridgeProvider;
import io.arenadata.dtm.query.execution.plugin.adg.service.TtCartridgeSchemaGenerator;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.exception.DdlDatasourceException;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlService;
import io.vertx.core.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class CreateTableExecutor implements DdlExecutor<Void> {

    private final DropTableExecutor dropTableExecutor;
    private final TtCartridgeSchemaGenerator generator;
    private final AdgCartridgeClient client;

    @Autowired
    public CreateTableExecutor(DropTableExecutor dropTableExecutor,
                               TtCartridgeSchemaGenerator generator,
                               AdgCartridgeClient client) {
        this.dropTableExecutor = dropTableExecutor;
        this.generator = generator;
        this.client = client;
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, AsyncHandler<Void> handler) {
        try {
            SqlNode query = context.getQuery();
            if (!(query instanceof SqlCreateTable)) {
                handler.handleError(new DdlDatasourceException(String.format("Expecting SqlCreateTable in context, receiving: %s",
                        context.getQuery())));
                return;
            }
            DdlRequestContext dropCtx = createDropRequestContext(context);
            dropTableExecutor.execute(dropCtx, SqlKind.DROP_TABLE.lowerName, ar -> {
                if (ar.succeeded()) {
                    Future.future((Promise<OperationYaml> promise) ->
                            generator.generate(context, new OperationYaml(), promise))
                            .compose(operationYaml -> Future.future((Promise<Void> promise) ->
                                    client.executeCreateSpacesQueued(operationYaml, promise)))
                            .onComplete(handler);
                } else {
                    handler.handleError(ar.cause());
                }
            });
        } catch (Exception e) {
            handler.handleError(new DdlDatasourceException("Error generating create table request", e));
        }
    }

    private DdlRequestContext createDropRequestContext(DdlRequestContext context) {
        return new DdlRequestContext(new DdlRequest(
                context.getRequest().getQueryRequest().copy(),
                context.getRequest().getEntity()));
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.CREATE_TABLE;
    }

    @Override
    @Autowired
    public void register(@Qualifier("adgDdlService") DdlService<Void> service) {
        service.addExecutor(this);
    }
}
