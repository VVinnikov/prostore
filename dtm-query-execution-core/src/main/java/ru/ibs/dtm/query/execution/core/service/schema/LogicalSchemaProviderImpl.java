package ru.ibs.dtm.query.execution.core.service.schema;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.dto.DatamartInfo;
import ru.ibs.dtm.common.dto.schema.DatamartSchemaKey;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.calcite.core.node.SqlSelectTree;
import ru.ibs.dtm.query.calcite.core.service.DefinitionService;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;
import ru.ibs.dtm.query.execution.model.metadata.DatamartTable;

import java.util.*;

@Service
@Slf4j
public class LogicalSchemaProviderImpl implements LogicalSchemaProvider {

    private final LogicalSchemaService logicalSchemaService;
    private final DefinitionService<SqlNode> definitionService;
    private final Map<DatamartSchemaKey, DatamartTable> datamartSchemaMap;

    @Autowired
    public LogicalSchemaProviderImpl(LogicalSchemaService logicalSchemaService,
                                     @Qualifier("coreCalciteDefinitionService") DefinitionService<SqlNode> definitionService) {
        this.logicalSchemaService = logicalSchemaService;
        this.definitionService = definitionService;
        this.datamartSchemaMap = new HashMap<>();
    }

    @Override
    public void getSchema(QueryRequest request, Handler<AsyncResult<List<Datamart>>> resultHandler) {
        try {
            final List<DatamartInfo> datamartInfoList = getDatamartInfoList(request);
            logicalSchemaService.createSchema(datamartInfoList, ar -> {
                if (ar.succeeded()) {
                    Map<DatamartSchemaKey, DatamartTable> datamartTableMap = ar.result();
                    datamartSchemaMap.putAll(datamartTableMap);
                    resultHandler.handle(Future.succeededFuture(getDatamartSchema(datamartSchemaMap)));
                } else {
                    log.error("Ошибка получения схемы данных для запроса: {}", request, ar.cause());
                    resultHandler.handle(Future.failedFuture(ar.cause()));
                }
            });
        } catch (Exception e) {
            log.error("Ошибка ", e);
            resultHandler.handle(Future.failedFuture(e));
        }
    }

    @NotNull
    private List<DatamartInfo> getDatamartInfoList(QueryRequest request) {
        val sqlNode = definitionService.processingQuery(request.getSql());
        val tree = new SqlSelectTree(sqlNode);
        val allTableAndSnapshots = tree.findAllTableAndSnapshots();
        Map<String, DatamartInfo> datamartMap = new HashMap<>();
        allTableAndSnapshots.forEach(node -> {
            if (node.getNode() instanceof SqlIdentifier) {
                //подразумевается, что на данном этапе в запросе уже будет проставлен defaultDatamart там, где требуется
                String schemaName = ((SqlIdentifier) node.getNode()).names.get(0);
                String tableName = ((SqlIdentifier) node.getNode()).names.get(1);
                DatamartInfo datamartInfo = datamartMap.getOrDefault(schemaName, new DatamartInfo(schemaName, new HashSet<>()));
                datamartInfo.getTables().add(tableName);
                datamartMap.putIfAbsent(datamartInfo.getSchemaName(), datamartInfo);
            } else {
                log.error("Некорректный тип sqlNode, ожидается SqlIdentifier");
            }
        });
        return new ArrayList<>(datamartMap.values());
    }

    @NotNull
    private List<Datamart> getDatamartSchema(Map<DatamartSchemaKey, DatamartTable> datamartSchemaMap) {
        Map<String, Datamart> datamartMap = new HashMap<>();
        datamartSchemaMap.forEach((k, v) -> {
            datamartMap.putIfAbsent(k.getSchema(), createDatamart(k.getSchema()));
            datamartMap.get(k.getSchema()).getDatamartTableClassesses().add(v);
        });
        return new ArrayList<>(datamartMap.values());
    }

    @NotNull
    private Datamart createDatamart(String schema) {
        Datamart datamart = new Datamart();
        datamart.setId(UUID.randomUUID());
        datamart.setMnemonic(schema);
        datamart.setDatamartTableClassesses(new ArrayList<>());
        return datamart;
    }

    @Override
    public void updateSchema(QueryRequest request, Handler<AsyncResult<List<Datamart>>> resultHandler) {
        //TODO implement
    }

}
