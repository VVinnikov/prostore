package io.arenadata.dtm.query.execution.core.ddl.service.impl;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.extension.ddl.SqlCreateTable;
import io.arenadata.dtm.query.execution.core.base.exception.datamart.DatamartNotExistsException;
import io.arenadata.dtm.query.execution.core.base.exception.datamart.DatamartNotExistsException;
import io.arenadata.dtm.query.execution.core.base.exception.entity.EntityAlreadyExistsException;
import io.arenadata.dtm.query.execution.core.base.exception.table.ValidationDtmException;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.base.service.metadata.MetadataCalciteGenerator;
import io.arenadata.dtm.query.execution.core.base.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.base.utils.InformationSchemaUtils;
import io.arenadata.dtm.query.execution.core.ddl.dto.DdlRequestContext;
import io.arenadata.dtm.query.execution.core.ddl.dto.DdlType;
import io.arenadata.dtm.query.execution.core.ddl.service.QueryResultDdlExecutor;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.arenadata.dtm.query.execution.core.ddl.utils.ValidationUtils.checkRequiredKeys;
import static io.arenadata.dtm.query.execution.core.ddl.utils.ValidationUtils.checkVarcharSize;

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
        val datamartName = getSchemaName(context.getDatamartName(), sqlNodeName);
        if (datamartName.equalsIgnoreCase(InformationSchemaUtils.INFORMATION_SCHEMA)) {
            return Future.failedFuture(new DtmException(String.format("Creating tables in schema [%s] is not supported",
                    InformationSchemaUtils.INFORMATION_SCHEMA)));
        }
        return createTable(context, datamartName);
    }

    private Future<QueryResult> createTable(DdlRequestContext context, String datamartName) {
        return Future.future(promise -> {
            context.getRequest().getQueryRequest().setDatamartMnemonic(datamartName);
            context.setDdlType(DdlType.CREATE_TABLE);
            SqlCreateTable sqlCreate = (SqlCreateTable) context.getSqlNode();
            val entity = metadataCalciteGenerator.generateTableMetadata(sqlCreate);
            entity.setEntityType(EntityType.TABLE);
            Set<SourceType> requestDestination = ((SqlCreateTable) context.getSqlNode()).getDestination();
            Set<SourceType> destination = Optional.ofNullable(requestDestination)
                    .orElse(dataSourcePluginService.getSourceTypes());
            entity.setDestination(destination);
            validateFields(entity.getFields());
            context.setEntity(entity);
            context.setDatamartName(datamartName);
            datamartDao.existsDatamart(datamartName)
                    .compose(isExistsDatamart -> isExistsDatamart ?
                            entityDao.existsEntity(datamartName, entity.getName()) : getNotExistsDatamartFuture(datamartName))
                    .compose(isExistsEntity -> isExistsEntity ?
                            getEntityAlreadyExistsFuture(entity.getNameWithSchema()) : createTable(context))
                    .onSuccess(success -> promise.complete(QueryResult.emptyResult()))
                    .onFailure(promise::fail);
        });
    }

    private void validateFields(List<EntityField> fields) {
        checkRequiredKeys(fields);
        checkVarcharSize(fields);
    }

    private void checkVarcharSize(List<EntityField> fields) {
        List<String> notSetSizeFields = fields.stream()
                .filter(field -> field.getType() == ColumnType.CHAR)
                .filter(field -> field.getSize() == null)
                .map(EntityField::getName)
                .collect(Collectors.toList());
        if (!notSetSizeFields.isEmpty()) {
            throw new ValidationDtmException(
                    String.format("Specifying the size for columns%s with types[VARCHAR, CHAR] is required", notSetSizeFields)
            );
        }
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
            throw new ValidationDtmException(
                    String.format("Primary keys and Sharding keys are required. The following keys do not exist: %s",
                            String.join(",", notExistsKeys)));
        }
    }

    private Future<Void> getEntityAlreadyExistsFuture(String entityNameWithSchema) {
        return Future.failedFuture(new EntityAlreadyExistsException(entityNameWithSchema));
    }

    private Future<Boolean> getNotExistsDatamartFuture(String datamartName) {
        return Future.failedFuture(new DatamartNotExistsException(datamartName));
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
    public Set<SqlKind> getSqlKinds() {
        return Collections.singleton(SqlKind.CREATE_TABLE);
    }
}
