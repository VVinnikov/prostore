package io.arenadata.dtm.query.execution.core.service.impl;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.InformationSchemaView;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.service.DdlQueryGenerator;
import io.arenadata.dtm.query.execution.core.service.InformationSchemaService;
import io.arenadata.dtm.query.execution.core.service.hsql.HSQLClient;
import io.arenadata.dtm.query.execution.core.utils.InformationSchemaUtils;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Component
public class InformationSchemaServiceImpl implements InformationSchemaService {

    private static final int TABLE_NAME_COLUMN_INDEX = 0;
    private static final int ORDINAL_POSITION_COLUMN_INDEX = 1;
    private static final int COLUMN_NAME_COLUMN_INDEX = 2;
    private static final int DATA_TYPE_COLUMN_INDEX = 3;
    private static final int IS_NULLABLE_COLUMN_INDEX = 4;
    private static final String IS_NULLABLE_COLUMN_TRUE = "YES";

    private HSQLClient client;
    private DatamartDao datamartDao;
    private EntityDao entityDao;
    private DdlQueryGenerator ddlQueryGenerator;
    private ApplicationContext applicationContext;
    private List<Entity> entities;

    public InformationSchemaServiceImpl(HSQLClient client,
                                        DatamartDao datamartDao,
                                        EntityDao entityDao,
                                        DdlQueryGenerator ddlQueryGenerator,
                                        ApplicationContext applicationContext) {
        this.client = client;
        this.datamartDao = datamartDao;
        this.entityDao = entityDao;
        this.ddlQueryGenerator = ddlQueryGenerator;
        this.applicationContext = applicationContext;
    }

    @PostConstruct
    public void init() {
        initialize();
    }

    @Override
    public void initialize() {
        createInformationSchemaViews()
                .compose(r -> createSchemasFromDatasource())
                .compose(a -> initLogicSchema())
                .onSuccess(success -> {
                    log.info("Information schema initialized successfully");
                })
                .onFailure(err -> {
                    log.error("Error while creating information schema views", err);
                    val exitCode = SpringApplication.exit(applicationContext, () -> 1);
                    System.exit(exitCode);
                });
    }

    @Override
    public List<Entity> getEntities()
    {
        return entities;
    }

    private Future<Void> initLogicSchema()
    {
        return Future.future(promise -> {
            String query = String.format("SELECT TABLE_NAME, ORDINAL_POSITION, COLUMN_NAME," +
                "  case" +
                "    when DATA_TYPE = 'DOUBLE PRECISION' then 'DOUBLE'" +
                "    when DATA_TYPE = 'CHARACTER VARYING' then 'VARCHAR'" +
                "    when DATA_TYPE = 'INTEGER' then 'INT'" +
                "    when DATA_TYPE = 'CHARACTER' then 'CHAR'" +
                "    else DATA_TYPE end as DATA_TYPE," +
                " IS_NULLABLE" +
                " FROM information_schema.columns WHERE TABLE_SCHEMA = '%s' and TABLE_NAME in (%s);",
                InformationSchemaView.DTM_SCHEMA_NAME,
                Arrays.stream(InformationSchemaView.values())
                    .map(view -> String.format("'%s'", view.getRealName().toUpperCase()))
                    .collect(Collectors.joining(",")));
            client.getQueryResult(query).onSuccess(resultSet -> {
                Map<String, List<EntityField>> fieldsByView = resultSet.getResults().stream()
                        .collect(Collectors.groupingBy(col -> col.getString(TABLE_NAME_COLUMN_INDEX),
                                Collectors.mapping(this::getField, Collectors.toList())));
                entities = Arrays.stream(InformationSchemaView.values())
                    .flatMap(view -> {
                        final String viewRealName = view.getRealName().toUpperCase();
                        return Optional.ofNullable(fieldsByView.get(viewRealName))
                            .map(fields -> getEntity(view, fields).stream())
                            .orElseThrow(() -> new RuntimeException(
                                String.format("View [%s.%s] doesn't exist", InformationSchemaView.DTM_SCHEMA_NAME,
                                    viewRealName)));
                    })
                    .collect(Collectors.toList());
                promise.complete();
            })
            .onFailure(promise::fail);
        });
    }

    private EntityField getField(final JsonArray jsonArray)
    {
        return new EntityField(jsonArray.getInteger(ORDINAL_POSITION_COLUMN_INDEX),
            jsonArray.getString(COLUMN_NAME_COLUMN_INDEX),
            ColumnType.valueOf(jsonArray.getString(DATA_TYPE_COLUMN_INDEX)),
            IS_NULLABLE_COLUMN_TRUE.equals(jsonArray.getString(IS_NULLABLE_COLUMN_INDEX)));
    }

    private List<Entity> getEntity(final InformationSchemaView view, final List<EntityField> fields)
    {
        return Arrays.asList(
            Entity.builder()
                .schema(InformationSchemaView.SCHEMA_NAME)
                .entityType(EntityType.VIEW)
                .name(view.name())
                .viewQuery(String.format("select * from %s.%s", InformationSchemaView.DTM_SCHEMA_NAME,
                    view.getRealName()))
                .fields(fields).build(),
            Entity.builder()
                .schema(InformationSchemaView.DTM_SCHEMA_NAME)
                .entityType(EntityType.TABLE)
                .name(view.getRealName())
                .fields(fields).build());
    }

    private Future<Void> createInformationSchemaViews() {
        return client.executeBatch(informationSchemaViewsQueries());
    }

    private List<String> informationSchemaViewsQueries() {
        return Arrays.asList(
                String.format(InformationSchemaUtils.CREATE_SCHEMA, InformationSchemaView.DTM_SCHEMA_NAME),
                InformationSchemaUtils.LOGIC_SCHEMA_DATAMARTS,
                InformationSchemaUtils.LOGIC_SCHEMA_ENTITIES,
                InformationSchemaUtils.LOGIC_SCHEMA_COLUMNS,
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

    private Future<List<Entity>> getEntitiesByNames(String datamart, List<String> entitiesNames) {
        return Future.future(promise -> {
            CompositeFuture.join(entitiesNames.stream()
                    .map(entity -> entityDao.getEntity(datamart, entity))
                    .collect(Collectors.toList()))
                    .onSuccess(entityResult -> promise.complete(entityResult.list()))
                    .onFailure(promise::fail);
        });
    }

    private List<String> getEntitiesCreateQueries(List<Entity> entities) {
        List<String> viewEntities = new ArrayList<>();
        List<String> tableEntities = new ArrayList<>();
        entities.forEach(entity -> {
            if (EntityType.VIEW.equals(entity.getEntityType())) {
                viewEntities.add(ddlQueryGenerator.generateCreateViewQuery(entity));
            }
            if (EntityType.TABLE.equals(entity.getEntityType())) {
                tableEntities.add(ddlQueryGenerator.generateCreateTableQuery(entity));
            }
        });
        return Stream.concat(tableEntities.stream(), viewEntities.stream())
                .collect(Collectors.toList());
    }
}
