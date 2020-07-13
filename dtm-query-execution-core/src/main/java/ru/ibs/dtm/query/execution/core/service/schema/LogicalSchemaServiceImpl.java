package ru.ibs.dtm.query.execution.core.service.schema;

import io.vertx.core.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.dto.DatamartInfo;
import ru.ibs.dtm.common.dto.schema.DatamartSchemaKey;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dto.metadata.DatamartEntity;
import ru.ibs.dtm.query.execution.core.dto.metadata.EntityAttribute;
import ru.ibs.dtm.query.execution.model.metadata.*;

import java.util.*;

@Service
@Slf4j
public class LogicalSchemaServiceImpl implements LogicalSchemaService {

    private final ServiceDbFacade serviceDbFacade;

    @Autowired
    public LogicalSchemaServiceImpl(ServiceDbFacade serviceDbFacade) {
        this.serviceDbFacade = serviceDbFacade;
    }

    @Override
    public void createSchema(List<DatamartInfo> tableInfoList, Handler<AsyncResult<Map<DatamartSchemaKey, DatamartTable>>> resultHandler) {
        try {
            final List<Future> datamartTableFutures = new ArrayList<>();
            final Map<DatamartSchemaKey, DatamartTable> datamartSchemaMap = new HashMap<>();
            tableInfoList.forEach(datamart -> {
                datamartTableFutures.add(getDatamartFuture(datamart));
            });
            CompositeFuture.join(datamartTableFutures)
                    .onComplete(ar -> {
                        if (ar.succeeded()) {
                            CompositeFuture tabFuture = ar.result();
                            final Map<String, DatamartTable> tableMap = new HashMap<>();
                            final List<Future> attributeFutures = initDatamartTables(tabFuture, datamartSchemaMap, tableMap);
                            CompositeFuture.join(attributeFutures)
                                    .onComplete(getDatamartTables(datamartSchemaMap, tableMap, resultHandler))
                                    .onFailure(Future::failedFuture);
                        } else {
                            resultHandler.handle(Future.failedFuture(ar.cause()));//TODO проверить
                        }
                    })
                    .onFailure(fail -> resultHandler.handle(Future.failedFuture(fail)));
        } catch (Exception e) {
            resultHandler.handle(Future.failedFuture(e));
        }
    }

    private Future<DatamartFuture> getDatamartFuture(DatamartInfo datamartInfo) {
        return Future.future((Promise<DatamartFuture> promise) -> {
            serviceDbFacade.getServiceDbDao().getEntityDao().findEntitiesByDatamartAndTableNames(datamartInfo, ar -> {
                if (ar.succeeded()) {
                    List<DatamartEntity> entities = ar.result();
                    final List<DatamartTableFuture> datamartTableFutures = new ArrayList<>();
                    entities.forEach(entity -> datamartTableFutures.add(new DatamartTableFuture(entity,
                            getDtatmartEntityAttributes(entity))));
                    promise.complete(new DatamartFuture(datamartInfo, datamartTableFutures));
                } else {
                    promise.fail(ar.cause());
                }
            });
        });
    }

    private Future<List<EntityAttribute>> getDtatmartEntityAttributes(DatamartEntity datamartEntity) {
        return Future.future((Promise<List<EntityAttribute>> promise) ->
                serviceDbFacade.getServiceDbDao().getAttributeDao().getAttributesMeta(datamartEntity.getDatamartMnemonic(),
                        datamartEntity.getMnemonic(), promise));
    }

    @NotNull
    private Handler<AsyncResult<CompositeFuture>> getDatamartTables(Map<DatamartSchemaKey, DatamartTable> datamartSchemaMap,
                                                                    Map<String, DatamartTable> tableMap,
                                                                    Handler<AsyncResult<Map<DatamartSchemaKey, DatamartTable>>> resultHandler) {
        return atr -> {
            if (atr.succeeded()) {
                CompositeFuture attrFuture = atr.result();
                try {
                    initTableAttributes(attrFuture, tableMap);
                    resultHandler.handle(Future.succeededFuture(datamartSchemaMap));
                } catch (Exception e) {
                    log.error("Ошибка инициализации атрибутов таблицы!", e);
                    resultHandler.handle(Future.failedFuture(atr.cause()));
                }
            } else {
                resultHandler.handle(Future.failedFuture(atr.cause()));//TODO проверить
            }
        };
    }

    private List<Future> initDatamartTables(CompositeFuture datamartFuture, Map<DatamartSchemaKey, DatamartTable> datamartSchemaMap,
                                            Map<String, DatamartTable> tableMap) {
        final List<Future> attrFutures = new ArrayList<>();
        try {
            datamartFuture.list().forEach(res -> {
                DatamartFuture dmFuture = (DatamartFuture) res;
                dmFuture.getTableFutures().forEach(tf -> {
                    final DatamartTable datamartTable = createDatamartTable(tf.getTable());
                    datamartSchemaMap.put(createDatamartSchemaKey(datamartTable), datamartTable);
                    tableMap.put(datamartTable.getLabel(), datamartTable);
                    attrFutures.add(tf.getAttributeFuture());
                });
            });
        } catch (Exception e) {
            log.error("Ошибка инициализации таблиц витрины!", e);
            throw new RuntimeException(e);
        }
        return attrFutures;
    }

    private void initTableAttributes(CompositeFuture attrFuture, Map<String, DatamartTable> tableMap) {
        attrFuture.list().forEach(entAttr -> {
            List<EntityAttribute> entityAttributes = (List<EntityAttribute>) entAttr;
            if (!entityAttributes.isEmpty()) {
                String tableLabel = entityAttributes.get(0).getEntityMnemonic();
                tableMap.get(tableLabel).setTableAttributes(createTableAttributes(entityAttributes));
            } else {
                throw new RuntimeException("Список атрибутов должен быть не пустой!");
            }
        });
    }

    @NotNull
    private DatamartSchemaKey createDatamartSchemaKey(DatamartTable datamartTable) {
        return new DatamartSchemaKey(datamartTable.getSchema(), datamartTable.getLabel());
    }

    @NotNull
    private Datamart createDatamart(DatamartInfo dm) {
        Datamart datamart = new Datamart();
        datamart.setId(UUID.randomUUID());
        datamart.setMnemonic(dm.getSchemaName());
        datamart.setDatamartTableClassesses(new ArrayList<>());
        return datamart;
    }

    @NotNull
    private DatamartTable createDatamartTable(DatamartEntity table) {
        final DatamartTable dmTable = new DatamartTable();
        dmTable.setId(UUID.randomUUID());
        dmTable.setSchema(table.getDatamartMnemonic());
        dmTable.setLabel(table.getMnemonic());
        dmTable.setTableAttributes(new ArrayList<>());
        return dmTable;
    }

    @NotNull
    private List<TableAttribute> createTableAttributes(List<EntityAttribute> attrs) {
        final List<TableAttribute> attributeList = new ArrayList<>();
        attrs.forEach(attr -> {
            final TableAttribute tableAttribute = new TableAttribute();
            tableAttribute.setId(UUID.randomUUID());
            tableAttribute.setMnemonic(attr.getMnemonic());
            tableAttribute.setType(mapColumnType(attr.getDataType()));
            attributeList.add(tableAttribute);
        });
        return attributeList;
    }

    private AttributeType mapColumnType(String dataType) {
        //FIXME переделать
        AttributeType attributeType = new AttributeType();
        attributeType.setId(UUID.randomUUID());
        ColumnType type = null;
        switch (dataType.toLowerCase()) {
            case "varchar":
            case "char":
                type = ColumnType.STRING;
                break;
            case "bigint":
                type = ColumnType.LONG;
                break;
            case "int":
            case "integer":
            case "tinyint":
                type = ColumnType.INTEGER;
                break;
            case "date":
                type = ColumnType.DATE;
                break;
            case "datetime":
            case "timestamp":
                type = ColumnType.TIMESTAMP;
                break;
            case "decimal":
            case "numeric":
                type = ColumnType.BIG_DECIMAL;
                break;
            case "float":
                type = ColumnType.FLOAT;
                break;
            case "double":
                type = ColumnType.DOUBLE;
                break;
            case "boolean":
                type = ColumnType.BOOLEAN;
                break;
            default:
                type = ColumnType.ANY;
                break;
        }
        attributeType.setValue(type);
        return attributeType;
    }

    @Data
    @AllArgsConstructor
    private static class DatamartFuture {
        private DatamartInfo datamartInfo;
        private List<DatamartTableFuture> tableFutures;
    }

    @Data
    @AllArgsConstructor
    private static class DatamartTableFuture {
        private DatamartEntity table;
        private Future attributeFuture;
    }
}
