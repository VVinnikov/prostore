package ru.ibs.dtm.query.execution.core.service.impl;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.common.model.ddl.EntityType;
import ru.ibs.dtm.query.calcite.core.service.HSQLQueryService;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import ru.ibs.dtm.query.execution.core.service.InformationSchemaService;
import ru.ibs.dtm.query.execution.core.service.hsql.HSQLClient;
import ru.ibs.dtm.query.execution.core.utils.InformationSchemaUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Component
public class InformationSchemaServiceImpl implements InformationSchemaService {

    private HSQLClient client;
    private DatamartDao datamartDao;
    private EntityDao entityDao;
    private HSQLQueryService queryService;
    private ApplicationContext applicationContext;

    public InformationSchemaServiceImpl(HSQLClient client, DatamartDao datamartDao, EntityDao entityDao, HSQLQueryService queryService, ApplicationContext applicationContext) {
        this.client = client;
        this.datamartDao = datamartDao;
        this.entityDao = entityDao;
        this.queryService = queryService;
        this.applicationContext = applicationContext;
        initialize();
    }

    @Override
    public void initialize(){
        createInformationSchemaViews().compose(r -> createSchemasFromDatasource())
                .onSuccess(success -> log.info("Inforamation schema initialized successfully"))
                .onFailure(err -> {
                    log.error("Error while creating information schema views", err);
                    val exitCode = SpringApplication.exit(applicationContext, () -> 1);
                    System.exit(exitCode);
                });
    }

    private Future<Void> createInformationSchemaViews(){
        return client.executeBatch(informationSchemaViewsQueries());
    }

    private List<String> informationSchemaViewsQueries(){
        return Arrays.asList(
                String.format(InformationSchemaUtils.CREATE_SCHEMA, "DTM"),
                InformationSchemaUtils.LOGIC_SCHEMA_DATAMARTS,
                InformationSchemaUtils.LOGIC_SCHEMA_ENTITIES,
                InformationSchemaUtils.LOGIC_SCHEMA_ATTRIBUTES,
                InformationSchemaUtils.LOGIC_SCHEMA_KEY_COLUMN_USAGE,
                InformationSchemaUtils.LOGIC_SCHEMA_ENTITY_CONSTRAINTS);
    }

    private Future<Void> createSchemasFromDatasource() {
        return datamartDao.getDatamarts()
                .compose(this::createSchemas);
    }

    private Future<Void> createSchemas(List<String> datamarts) {
        return Future.future(p -> {
            CompositeFuture.join(datamarts.stream()
                    .map(this::createSchemaForDatamart)
                    .collect(Collectors.toList()))
                    .onSuccess(success -> p.complete())
                    .onFailure(p::fail);
        });
    }

    private Future<Void> createSchemaForDatamart(String datamart) {
        val query = String.format(InformationSchemaUtils.CREATE_SCHEMA, datamart);
        return client.executeQuery(query)
                .compose(r -> entityDao.getEntityNamesByDatamart(datamart))
                .compose(entityNames -> getEntitiesByNames(datamart, entityNames))
                .compose(entities -> client.executeBatch(getEntitiesCreateQueries(entities)));
    }

    private Future<List<Entity>> getEntitiesByNames(String datamart, List<String> entitiesNames){
        return Future.future(promise -> {
            CompositeFuture.join(entitiesNames.stream()
                    .map(entity -> entityDao.getEntity(datamart, entity))
                    .collect(Collectors.toList()))
                    .onSuccess(entityResult -> promise.complete(entityResult.list()))
                    .onFailure(promise::fail);
        });
    }

    private List<String> getEntitiesCreateQueries(List<Entity> entities){
        List<String> viewEntities = new ArrayList<>();
        List<String> tableEntities = new ArrayList<>();
        entities.forEach(entity -> {
            if (EntityType.VIEW.equals(entity.getEntityType())) {
                viewEntities.add(queryService.generateCreateViewQuery(entity));
            }
            if (EntityType.TABLE.equals(entity.getEntityType())) {
                tableEntities.add(queryService.generateCreateTableQuery(entity));
            }
        });
        return Stream.concat(tableEntities.stream(), viewEntities.stream())
                .collect(Collectors.toList());
    }
}
