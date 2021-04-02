package io.arenadata.dtm.query.execution.plugin.adg.ddl.service;

import io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.OperationYaml;
import io.arenadata.dtm.query.execution.plugin.adg.base.service.client.AdgCartridgeClient;
import io.arenadata.dtm.query.execution.plugin.adg.base.service.client.AdgCartridgeSchemaGenerator;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.DdlExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.DdlService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class CreateTableExecutor implements DdlExecutor<Void> {

    private final DropTableExecutor dropTableExecutor;
    private final AdgCartridgeSchemaGenerator generator;
    private final AdgCartridgeClient client;

    @Autowired
    public CreateTableExecutor(DropTableExecutor dropTableExecutor,
                               AdgCartridgeSchemaGenerator generator,
                               AdgCartridgeClient client) {
        this.dropTableExecutor = dropTableExecutor;
        this.generator = generator;
        this.client = client;
    }

    @Override
    public Future<Void> execute(DdlRequest request) {
        return Future.future(promise -> {
            dropTableExecutor.execute(request)
                    .compose(result -> generator.generate(request, new OperationYaml()))
                    .compose(client::executeCreateSpacesQueued)
                    .onComplete(promise);
        });
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
