package ru.ibs.dtm.query.execution.core.service.impl;


import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dto.metadata.DatamartEntity;
import ru.ibs.dtm.query.execution.core.service.SchemaStorageProvider;
import ru.ibs.dtm.query.execution.model.metadata.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
@Slf4j
public class SchemaStorageProviderImpl implements SchemaStorageProvider {
    private final ServiceDbFacade serviceDbFacade;

    public SchemaStorageProviderImpl(ServiceDbFacade serviceDbFacade) {
        this.serviceDbFacade = serviceDbFacade;
    }

    @Override
    public void getLogicalSchema(String datamartMnemonic, Handler<AsyncResult<JsonObject>> asyncResultHandler) {
        serviceDbFacade.getServiceDbDao().getEntityDao().getEntitiesMeta(datamartMnemonic, ar -> {
            if (ar.succeeded()) {
                List<DatamartEntity> dmEntity = ar.result();
                List<DatamartTable> dmClassResult = new ArrayList<>();
                fillDmArray(dmEntity.size() - 1, dmEntity, dmClassResult, ar2 -> {
                    if (ar2.succeeded()) {
                        Datamart datamart = new Datamart();
                        datamart.setId(UUID.randomUUID());
                        datamart.setMnemonic(datamartMnemonic);
                        datamart.setDatamartTableClassesses(ar2.result());
                        JsonObject jsonResult = new JsonObject(Json.encode(datamart));
                        log.info("Метаданные для запроса получены. Схема: {}", datamartMnemonic);
                        log.trace("Метаданные: [{}]", jsonResult.toString());
                        asyncResultHandler.handle(Future.succeededFuture(jsonResult));
                    } else {
                        asyncResultHandler.handle(Future.failedFuture(ar2.cause()));
                    }
                });
            } else {
                asyncResultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    private void fillDmArray(int currentIteration, List<DatamartEntity> dmEntities, List<DatamartTable> dmClassResult, Handler<AsyncResult<List<DatamartTable>>> asyncResult) {
        if (currentIteration < 0) {
            asyncResult.handle(Future.succeededFuture(dmClassResult));
        } else {
            DatamartEntity datamartEntity = dmEntities.get(currentIteration);
            serviceDbFacade.getServiceDbDao().getAttributeDao().getAttributesMeta(datamartEntity.getDatamartMnemonic(), datamartEntity.getMnemonic(), ar -> {
                if (ar.succeeded()) {
                    List<TableAttribute> tableAttributes = new ArrayList<>();
                    DatamartTable dmClass = new DatamartTable();
                    dmClass.setId(UUID.randomUUID());
                    dmClass.setSchema(datamartEntity.getMnemonic());
                    dmClass.setLabel(datamartEntity.getMnemonic());
                    ar.result().forEach(attr -> {
                        TableAttribute tableAttribute = new TableAttribute();
                        tableAttribute.setId(UUID.randomUUID());
                        tableAttribute.setMnemonic(attr.getMnemonic());
                        tableAttribute.setType(mapColumnType(attr.getDataType()));
                        tableAttributes.add(tableAttribute);
                    });
                    dmClass.setTableAttributes(tableAttributes);
                    dmClassResult.add(dmClass);
                    fillDmArray(currentIteration - 1, dmEntities, dmClassResult, asyncResult);
                } else {
                    asyncResult.handle(Future.failedFuture(ar.cause()));
                }
            });
        }
    }

    private AttributeType mapColumnType(String dataType) {
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
}
