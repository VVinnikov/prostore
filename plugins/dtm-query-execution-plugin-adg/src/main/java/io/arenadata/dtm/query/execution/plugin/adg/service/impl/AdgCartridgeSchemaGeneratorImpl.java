package io.arenadata.dtm.query.execution.plugin.adg.service.impl;

import io.arenadata.dtm.query.execution.plugin.adg.dto.AdgTables;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.OperationYaml;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.schema.AdgSpace;
import io.arenadata.dtm.query.execution.plugin.adg.service.AdgCartridgeSchemaGenerator;
import io.arenadata.dtm.query.execution.plugin.api.factory.CreateTableQueriesFactory;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.vertx.core.Future;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.stream.Stream;

@Service
public class AdgCartridgeSchemaGeneratorImpl implements AdgCartridgeSchemaGenerator {
    private final CreateTableQueriesFactory<AdgTables<AdgSpace>> createTableQueriesFactory;

    @Autowired
    public AdgCartridgeSchemaGeneratorImpl(CreateTableQueriesFactory<AdgTables<AdgSpace>> createTableQueriesFactory) {
        this.createTableQueriesFactory = createTableQueriesFactory;
    }

    @Override
    public Future<OperationYaml> generate(DdlRequest request, OperationYaml yaml) {
        return Future.future(promise -> {
            if (yaml.getSpaces() == null) {
                yaml.setSpaces(new LinkedHashMap<>());
            }
            val spaces = yaml.getSpaces();
            AdgTables<AdgSpace> adgCreateTableQueries = createTableQueriesFactory.create(request.getEntity(), request.getEnvName());
            Stream.of(adgCreateTableQueries.getActual(), adgCreateTableQueries.getHistory(),
                    adgCreateTableQueries.getStaging())
                    .forEach(space -> spaces.put(space.getName(), space.getSpace()));
            promise.complete(yaml);
        });
    }

}
