package io.arenadata.dtm.query.execution.core.service.ddl.impl;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.extension.ddl.SqlCreateTable;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.exception.datamart.DatamartNotExistsException;
import io.arenadata.dtm.query.execution.core.exception.table.TableAlreadyExistsException;
import io.arenadata.dtm.query.execution.core.service.datasource.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.ddl.QueryResultDdlExecutor;
import io.arenadata.dtm.query.execution.core.service.metadata.MetadataCalciteGenerator;
import io.arenadata.dtm.query.execution.core.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.dto.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.core.dto.ddl.DdlType;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Slf4j
@Component
public class CreateTableDdlExecutor extends QueryResultDdlExecutor {

    private final MetadataCalciteGenerator metadataCalciteGenerator;
    private final DatamartDao datamartDao;
    private final EntityDao entityDao;
    private final DataSourcePluginService dataSourcePluginService;

    @Autowired
    public CreateTableDdlExecutor(MetadataExecutor<DdlRequestContext> metadataExecutor,
                                  ServiceDbFacade serviceDbFacade,
                                  MetadataCalciteGenerator metadataCalciteGenerator,
                                  DataSourcePluginService dataSourcePluginService) {
        super(metadataExecutor, serviceDbFacade);
        this.metadataCalciteGenerator = metadataCalciteGenerator;
        datamartDao = serviceDbFacade.getServiceDbDao().getDatamartDao();
        entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
        this.dataSourcePluginService = dataSourcePluginService;
    }

    @Override
    public Future<QueryResult> execute(DdlRequestContext context, String sqlNodeName) {
        return createTable(context, sqlNodeName);
    }

    private Future<QueryResult> createTable(DdlRequestContext context, String sqlNodeName) {
        return Future.future(promise -> {
            val datamartName = getSchemaName(context.getRequest().getQueryRequest(), sqlNodeName);
            context.getRequest().getQueryRequest().setDatamartMnemonic(datamartName);
            context.setDdlType(DdlType.CREATE_TABLE);
            SqlCreateTable sqlCreate = (SqlCreateTable) context.getSqlNode();
            val entity = metadataCalciteGenerator.generateTableMetadata(sqlCreate);
            entity.setEntityType(EntityType.TABLE);
            Set<SourceType> requestDestination = ((SqlCreateTable) context.getSqlNode()).getDestination();
            Set<SourceType> destination = Optional.ofNullable(requestDestination)
                    .orElse(dataSourcePluginService.getSourceTypes());
            entity.setDestination(destination);
            checkRequiredKeys(entity.getFields());
            context.setEntity(entity);
            context.setDatamartName(datamartName);
            datamartDao.existsDatamart(datamartName)
                    .compose(isExistsDatamart -> isExistsDatamart ?
                            entityDao.existsEntity(datamartName, entity.getName()) : getNotExistsDatamartFuture(datamartName))
                    .onSuccess(isExistsEntity -> createTableIfNotExists(context, isExistsEntity)
                            .onSuccess(success -> {
                                promise.complete(QueryResult.emptyResult());
                            })
                            .onFailure(promise::fail))
                    .onFailure(promise::fail);
        });
    }

    private void checkRequiredKeys(List<EntityField> fields) {
        val notExistsKeys = new ArrayList<String>();
        val notExistsPrimaryKeys = fields.stream()
                .noneMatch(f -> f.getPrimaryOrder() != null);
        if (notExistsPrimaryKeys) {
            notExistsKeys.add("primary key(s)");
        }

        val notExistsShardingKey = fields.stream()
                .noneMatch(f -> f.getShardingOrder() != null);
        if (notExistsShardingKey) {
            notExistsKeys.add("sharding key(s)");
        }

        if (!notExistsKeys.isEmpty()) {
            throw new DtmException(
                    String.format("Primary keys and Sharding keys are required. The following keys do not exist: %s",
                            String.join(",", notExistsKeys)));
        }
    }

    private Future<Boolean> getNotExistsDatamartFuture(String datamartName) {
        return Future.failedFuture(new DatamartNotExistsException(datamartName));
    }

    private Future<Void> createTableIfNotExists(DdlRequestContext context,
                                                Boolean isTableExists) {
        if (isTableExists) {
            return Future.failedFuture(
                    new TableAlreadyExistsException(context.getEntity().getNameWithSchema()));
        } else {
            return createTable(context);
        }
    }

    private Future<Void> createTable(DdlRequestContext context) {
        //creating tables in data sources through plugins
        return Future.future((Promise<Void> promise) -> {
            metadataExecutor.execute(context)
                    .compose(v -> entityDao.createEntity(context.getEntity()))
                    .onSuccess(ar2 -> {
                        log.debug("Table [{}] in datamart [{}] successfully created",
                                context.getEntity().getName(),
                                context.getDatamartName());
                        promise.complete();
                    })
                    .onFailure(promise::fail);
        });
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.CREATE_TABLE;
    }
}
