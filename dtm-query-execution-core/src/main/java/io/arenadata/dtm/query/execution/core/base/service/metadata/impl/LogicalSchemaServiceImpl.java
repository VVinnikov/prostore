package io.arenadata.dtm.query.execution.core.base.service.metadata.impl;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.dto.DatamartInfo;
import io.arenadata.dtm.common.dto.schema.DatamartSchemaKey;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.calcite.core.service.DeltaInformationExtractor;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.base.service.metadata.LogicalSchemaService;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@Slf4j
public class LogicalSchemaServiceImpl implements LogicalSchemaService {

    private final EntityDao entityDao;
    private final DeltaInformationExtractor deltaInformationExtractor;

    @Autowired
    public LogicalSchemaServiceImpl(ServiceDbFacade serviceDbFacade,
                                    DeltaInformationExtractor deltaInformationExtractor) {
        this.entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
        this.deltaInformationExtractor = deltaInformationExtractor;
    }

    @Override
    public Future<Map<DatamartSchemaKey, Entity>> createSchemaFromQuery(SqlNode query) {
        return Future.future(promise -> {
            final List<DatamartInfo> datamartInfoList = getDatamartInfoListFromQuery(query);
            createSchema(promise, datamartInfoList);
        });
    }

    @Override
    public Future<Map<DatamartSchemaKey, Entity>> createSchemaFromDeltaInformations(List<DeltaInformation> deltaInformations) {
        return Future.future(promise -> {
            final List<DatamartInfo> datamartInfoList = getDatamartInfoListFromDeltaInformations(deltaInformations);
            createSchema(promise, datamartInfoList);
        });
    }

    private List<DatamartInfo> getDatamartInfoListFromQuery(SqlNode query) {
        val tree = new SqlSelectTree(query);
        val datamartMap = new HashMap<String, DatamartInfo>();
        tree.findAllTableAndSnapshots().stream()
                .map(node -> deltaInformationExtractor.getDeltaInformation(tree, node))
                .filter(Objects::nonNull)
                .forEach(node -> {
                    //it is assumed that at this stage, the request will already contain defaultDatamart where required
                    String schemaName = node.getSchemaName();
                    String tableName = node.getTableName();
                    DatamartInfo datamartInfo = datamartMap.getOrDefault(schemaName, new DatamartInfo(schemaName, new HashSet<>()));
                    datamartInfo.getTables().add(tableName);
                    datamartMap.putIfAbsent(datamartInfo.getSchemaName(), datamartInfo);
                });
        return new ArrayList<>(datamartMap.values());
    }

    private List<DatamartInfo> getDatamartInfoListFromDeltaInformations(List<DeltaInformation> deltaInformations) {
        val datamartMap = new HashMap<String, DatamartInfo>();
        deltaInformations
                .forEach(d -> {
                    DatamartInfo datamartInfo = datamartMap.getOrDefault(d.getSchemaName(),
                            new DatamartInfo(d.getSchemaName(), new HashSet<>()));
                    datamartInfo.getTables().add(d.getTableName());
                    datamartMap.putIfAbsent(datamartInfo.getSchemaName(), datamartInfo);
                });
        return new ArrayList<>(datamartMap.values());
    }

    private void createSchema(Promise<Map<DatamartSchemaKey, Entity>> promise,
                              List<DatamartInfo> datamartInfoList) {
        CompositeFuture.join(
                datamartInfoList.stream()
                        .flatMap(di -> di.getTables().stream()
                                .map(tableName -> new DatamartSchemaKey(di.getSchemaName(), tableName)))
                        .map(dsKey -> entityDao.getEntity(dsKey.getSchema(), dsKey.getTable()))
                        .collect(Collectors.toList()))
                .onSuccess(success -> {
                    List<Entity> entities = success.list();
                    val schemaKeyDatamartTableMap = entities.stream()
                            .map(Entity::copy)
                            .collect(Collectors.toMap(this::createDatamartSchemaKey, Function.identity()));
                    promise.complete(schemaKeyDatamartTableMap);
                })
                .onFailure(error -> promise.fail(new DtmException("Error initializing table attributes", error)));
    }

    private DatamartSchemaKey createDatamartSchemaKey(Entity table) {
        return new DatamartSchemaKey(table.getSchema(), table.getName());
    }

}
