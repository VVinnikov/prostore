package io.arenadata.dtm.query.execution.plugin.adg.base.service.client.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.OperationYaml;
import io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.response.ResOperation;
import io.arenadata.dtm.query.execution.plugin.adg.base.service.client.AdgCartridgeClient;
import io.arenadata.dtm.query.execution.plugin.adg.base.service.client.AdgCartridgeProvider;
import io.arenadata.dtm.query.execution.plugin.adg.base.service.client.AdgCartridgeSchemaGenerator;
import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
public class AdgCartridgeProviderImpl implements AdgCartridgeProvider {

    private final AdgCartridgeClient client;
    private final AdgCartridgeSchemaGenerator generator;
    private final ObjectMapper yamlMapper;

    @Autowired
    public AdgCartridgeProviderImpl(AdgCartridgeClient client,
                                    AdgCartridgeSchemaGenerator generator,
                                    @Qualifier("yamlMapper") ObjectMapper yamlMapper) {
        this.client = client;
        this.generator = generator;
        this.yamlMapper = yamlMapper;
    }

    @Override
    public Future<Void> apply(final DdlRequest request) {
        return applySchema(request);
    }

    public Future<Void> applySchema(final DdlRequest context) {
        return Future.future(promise -> client.getSchema()
                .compose(resOperation -> generateYaml(context, resOperation))
                .compose(this::createYamlString)
                .compose(client::setSchema)
                .onComplete(success -> promise.complete())
                .onFailure(promise::fail));
    }

    private Future<OperationYaml> generateYaml(DdlRequest context, ResOperation resultOperation) {
        return Future.future((Promise<OperationYaml> promise) -> {
            try {
                val yaml = yamlMapper.readValue(
                        resultOperation.getData().getCluster().getSchema().getYaml(),
                        OperationYaml.class);
                generator.generate(context, yaml)
                        .onComplete(promise);
            } catch (Exception ex) {
                promise.fail(new DataSourceException("Error in generating yaml", ex));
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
                    promise.fail(new DataSourceException("Empty generated yaml config"));
                }
            } catch (Exception ex) {
                promise.fail(new DataSourceException("Error in serializing yaml to string", ex));
            }
        });
    }
}
