package io.arenadata.dtm.query.execution.core.service.schema.impl;

import io.arenadata.dtm.common.dto.DatamartInfo;
import io.arenadata.dtm.common.dto.schema.DatamartSchemaKey;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.service.schema.LogicalSchemaService;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@Slf4j
public class LogicalSchemaServiceImpl implements LogicalSchemaService {

    private final EntityDao entityDao;

    @Autowired
    public LogicalSchemaServiceImpl(ServiceDbFacade serviceDbFacade) {
        this.entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
    }

    @Override
    public Future<Map<DatamartSchemaKey, Entity>> createSchema(QueryRequest request) {
        return Future.future(promise -> {
            final List<DatamartInfo> datamartInfoList = getDatamartInfoListFromRequest(request);
            CompositeFuture.join(
                    datamartInfoList.stream()
                            .flatMap(di -> di.getTables().stream()
                                    .map(tableName -> new DatamartSchemaKey(di.getSchemaName(), tableName)))
                            .map(dsKey -> entityDao.getEntity(dsKey.getSchema(), dsKey.getTable()))
                            .collect(Collectors.toList()))
                    .onSuccess(success -> {
                        List<Entity> entities = success.list();
                        val schemaKeyDatamartTableMap = entities.stream()
                                .map(Entity::clone)
                                .collect(Collectors.toMap(this::createDatamartSchemaKey, Function.identity()));
                        promise.complete(schemaKeyDatamartTableMap);
                    })
                    .onFailure(error -> promise.fail(new DtmException("Error initializing table attributes", error)));
        });
    }

    private List<DatamartInfo> getDatamartInfoListFromRequest(QueryRequest request) {
        val datamartMap = new HashMap<String, DatamartInfo>();
        request.getDeltaInformations()
                .forEach(d -> {
                    DatamartInfo datamartInfo = datamartMap.getOrDefault(d.getSchemaName(),
                            new DatamartInfo(d.getSchemaName(), new HashSet<>()));
                    datamartInfo.getTables().add(d.getTableName());
                    datamartMap.putIfAbsent(datamartInfo.getSchemaName(), datamartInfo);
                });
        return new ArrayList<>(datamartMap.values());
    }

    private DatamartSchemaKey createDatamartSchemaKey(Entity table) {
        return new DatamartSchemaKey(table.getSchema(), table.getName());
    }

}
