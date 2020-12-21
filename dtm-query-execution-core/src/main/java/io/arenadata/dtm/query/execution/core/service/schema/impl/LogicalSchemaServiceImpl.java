package io.arenadata.dtm.query.execution.core.service.schema.impl;

import io.arenadata.dtm.common.dto.DatamartInfo;
import io.arenadata.dtm.common.dto.schema.DatamartSchemaKey;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.calcite.core.service.DeltaInformationExtractor;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.query.execution.core.service.schema.LogicalSchemaService;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@Slf4j
public class LogicalSchemaServiceImpl implements LogicalSchemaService {

    private final DefinitionService<SqlNode> definitionService;
    private final EntityDao entityDao;
    private final DeltaInformationExtractor deltaInformationExtractor;

    @Autowired
    public LogicalSchemaServiceImpl(ServiceDbFacade serviceDbFacade,
                                    @Qualifier("coreCalciteDefinitionService") DefinitionService<SqlNode> definitionService,
                                    DeltaInformationExtractor deltaInformationExtractor) {
        this.definitionService = definitionService;
        this.entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
        this.deltaInformationExtractor = deltaInformationExtractor;
    }

    @Override
    public Future<Map<DatamartSchemaKey, Entity>> createSchema(QueryRequest request) {
        return Future.future(promise -> {
            final List<DatamartInfo> datamartInfoList = getDatamartInfoListFromQuery(request.getSql());
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
                    .onFailure(error -> {
                        promise.fail(new DtmException("Error initializing table attributes", error));
                    });
        });
    }

    private List<DatamartInfo> getDatamartInfoListFromQuery(String sql) {
        val sqlNode = definitionService.processingQuery(sql);
        val tree = new SqlSelectTree(sqlNode);
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

    private DatamartSchemaKey createDatamartSchemaKey(Entity table) {
        return new DatamartSchemaKey(table.getSchema(), table.getName());
    }

}
