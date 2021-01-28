package io.arenadata.dtm.query.execution.plugin.adg.service.impl.ddl;

import io.arenadata.dtm.query.execution.plugin.adg.factory.AdgHelperTableNamesFactory;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.request.TtDeleteTablesRequest;
import io.arenadata.dtm.query.execution.plugin.adg.service.AdgCartridgeClient;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
@Slf4j
public class DropTableExecutor implements DdlExecutor<Void> {

    private final AdgCartridgeClient cartridgeClient;
    private final AdgHelperTableNamesFactory adgHelperTableNamesFactory;

    @Autowired
    public DropTableExecutor(AdgCartridgeClient cartridgeClient,
                             AdgHelperTableNamesFactory adgHelperTableNamesFactory) {
        this.cartridgeClient = cartridgeClient;
        this.adgHelperTableNamesFactory = adgHelperTableNamesFactory;
    }

    @Override
    public Future<Void> execute(DdlRequestContext context, String sqlNodeName) {
        return Future.future(promise -> {
            val tableNames = adgHelperTableNamesFactory.create(
                    context.getRequest().getQueryRequest().getEnvName(),
                    context.getRequest().getQueryRequest().getDatamartMnemonic(),
                    context.getRequest().getEntity().getName());

            val request = new TtDeleteTablesRequest(Arrays.asList(
                    tableNames.getStaging(),
                    tableNames.getActual(),
                    tableNames.getHistory()
            ));
            cartridgeClient.executeDeleteSpacesQueued(request)
                    .onComplete(promise);
        });
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.DROP_TABLE;
    }

    @Override
    @Autowired
    public void register(@Qualifier("adgDdlService") DdlService<Void> service) {
        service.addExecutor(this);
    }
}
