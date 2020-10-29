package io.arenadata.dtm.query.execution.core.service.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.query.calcite.core.extension.ddl.SqlCreateTable;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.service.DdlQueryGenerator;
import io.arenadata.dtm.query.execution.core.service.InformationSchemaService;
import io.arenadata.dtm.query.execution.core.service.hsql.HSQLClient;
import io.arenadata.dtm.query.execution.core.utils.InformationSchemaUtils;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Component
public class InformationSchemaServiceImpl implements InformationSchemaService {

    private static final String DOUBLE_TYPE = "DOUBLE";
    private static final String FLOAT_TYPE = "FLOAT";
    private static final String VARCHAR_TYPE = "VARCHAR";
    private static final String INT_TYPE = "INT";

    private HSQLClient client;
    private DatamartDao datamartDao;
    private EntityDao entityDao;
    private DdlQueryGenerator ddlQueryGenerator;
    private ApplicationContext applicationContext;

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
    public void update(SqlCall sql) {
        switch (sql.getKind()) {
            case CREATE_TABLE:
                createTable((SqlCreateTable) sql);
                return;
            case CREATE_SCHEMA:
            case DROP_SCHEMA:
                client.executeQuery(sql.toString().replace("DATABASE", "SCHEMA").replace("`", ""))
                        .onFailure(err -> shutdown(err));
                return;
            case CREATE_VIEW:
            case ALTER_VIEW:
            case DROP_VIEW:
            case DROP_TABLE:
                client.executeQuery(sql.toString().replace("`", ""))
                        .onFailure(err -> shutdown(err));
                return;
            default:
                return;
        }
    }

    private void createTable(SqlCreateTable createTable) {
        val distributedByColumns = createTable.getDistributedBy().getDistributedBy().getList().stream().map(SqlNode::toString).collect(Collectors.toList());
        val schemaTable = createTable.getOperandList().get(0).toString();
        val table = getTableName(schemaTable);
        val creatTableQuery = sqlWithoutDistributedBy(createTable);

        List<String> commentQueries = new ArrayList<>();
        val columns = ((SqlNodeList) createTable.getOperandList().get(1)).getList();
        columns.stream().filter(node -> node instanceof SqlColumnDeclaration).map(node -> (SqlColumnDeclaration) node)
                .forEach(column -> {
                    val name = column.getOperandList().get(0).toString();
                    val type = column.getOperandList().get(1).toString();
                    switch (type) {
                        case DOUBLE_TYPE:
                        case FLOAT_TYPE:
                        case INT_TYPE:
                        case VARCHAR_TYPE:
                            commentQueries.add(commentOnColumn(schemaTable, name, type));
                            break;
                        default:
                            break;
                    }
                });

        client.executeQuery(creatTableQuery)
                .compose(r -> client.executeQuery(createShardingKeyIndex(table, schemaTable, distributedByColumns)))
                .compose(r -> client.executeBatch(commentQueries))
                .onFailure(err -> shutdown(err));
    }

    private String sqlWithoutDistributedBy(SqlCreateTable createTable) {
        String sqlString = createTable.toString();
        return sqlString.substring(0, sqlString.indexOf("DISTRIBUTED BY") - 1).replace("`", "");
    }

    private String commentOnColumn(String schemaTable, String column, String comment) {
        return String.format(InformationSchemaUtils.COMMENT_ON_COLUMN, schemaTable, column, comment);
    }

    private String createShardingKeyIndex(String table, String schemaTable, List<String> columns) {
        return String.format(InformationSchemaUtils.CREATE_SHARDING_KEY_INDEX,
                table, schemaTable, String.join(", ", columns));
    }

    private String getTableName(String schemaTable) {
        return schemaTable.substring(schemaTable.indexOf(".") + 1);
    }

    @Override
    public void initialize() {
        createInformationSchemaViews()
                .compose(r -> createSchemasFromDatasource())
                .onSuccess(success -> log.info("Information schema initialized successfully"))
                .onFailure(err -> shutdown(err));
    }

    private void shutdown(Throwable err) {
        log.error("Error while creating/updating information schema", err);
        val exitCode = SpringApplication.exit(applicationContext, () -> 1);
        System.exit(exitCode);
    }

    private Future<Void> createInformationSchemaViews() {
        return client.executeBatch(informationSchemaViewsQueries());
    }

    private List<String> informationSchemaViewsQueries() {
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
