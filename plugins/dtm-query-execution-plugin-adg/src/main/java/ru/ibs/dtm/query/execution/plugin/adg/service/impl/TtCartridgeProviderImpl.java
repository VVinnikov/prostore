package ru.ibs.dtm.query.execution.plugin.adg.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.SneakyThrows;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.OperationYaml;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.response.ResOperation;
import ru.ibs.dtm.query.execution.plugin.adg.service.AdgCartridgeClient;
import ru.ibs.dtm.query.execution.plugin.adg.service.TtCartridgeProvider;
import ru.ibs.dtm.query.execution.plugin.adg.service.TtCartridgeSchemaGenerator;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;

@Service
public class TtCartridgeProviderImpl implements TtCartridgeProvider {

    private AdgCartridgeClient client;
    private TtCartridgeSchemaGenerator generator;
    private ObjectMapper yamlMapper;

    @Autowired
    public TtCartridgeProviderImpl(AdgCartridgeClient client, TtCartridgeSchemaGenerator generator, @Qualifier("yamlMapper") ObjectMapper yamlMapper) {
        this.client = client;
        this.generator = generator;
        this.yamlMapper = yamlMapper;
    }

    @Override
    public void apply(final DdlRequestContext context, final Handler<AsyncResult<Void>> handler) {
        applySchema(context, ar -> {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture());
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @SneakyThrows
    public void applySchema(final DdlRequestContext context, final Handler<AsyncResult<Void>> handler) {
        Future.future((Promise<ResOperation> promise) -> client.getSchema(promise))
                .compose(resOperation -> generateYaml(context, resOperation))
                .compose(this::createYamlString)
                .compose(ys -> Future.future((Promise<ResOperation> promise) -> client.setSchema(ys, promise)))
                .onSuccess(success -> handler.handle(Future.succeededFuture()))
                .onFailure(failure -> handler.handle(Future.failedFuture(failure)));
    }

    private Future<OperationYaml> generateYaml(DdlRequestContext context, ResOperation resultOperation) {
        return Future.future((Promise<OperationYaml> promise) -> {
            try {
                val yaml = yamlMapper.readValue(
                        resultOperation.getData().getCluster().getSchema().getYaml(),
                        OperationYaml.class);
                generator.generate(context, yaml, promise);
            } catch (Exception ex) {
                promise.fail(ex);
            }
        });
    }

    private Future<String> createYamlString(OperationYaml yaml) {
        return Future.future((Promise<String> promise) -> {
            try {
                val yamlResult = yamlMapper.writeValueAsString(yaml);
                if (!yamlResult.isEmpty()) {
                    promise.complete(yamlResult);
                } else {
                    promise.fail("Empty generated yaml config");
                }
            } catch (Exception ex) {
                promise.fail(ex);
            }
        });
    }
}
