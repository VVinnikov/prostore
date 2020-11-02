package io.arenadata.dtm.query.execution.core.service.impl;

import io.arenadata.dtm.common.dto.TableInfo;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.dto.delta.DeltaWriteOp;
import io.arenadata.dtm.query.execution.core.service.RestoreStateService;
import io.arenadata.dtm.query.execution.core.service.edml.EdmlUploadFailedExecutor;
import io.arenadata.dtm.query.execution.core.service.edml.impl.UploadExternalTableExecutor;
import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Component
public class RestoreStateServiceImpl implements RestoreStateService {

    private final DatamartDao datamartDao;
    private final EntityDao entityDao;
    private final DeltaServiceDao deltaServiceDao;
    private final EdmlUploadFailedExecutor edmlUploadFailedExecutor;
    private final UploadExternalTableExecutor uploadExternalTableExecutor;
    private final DefinitionService<SqlNode> definitionService;
    private final String envName;

    @Autowired
    public RestoreStateServiceImpl(ServiceDbFacade serviceDbFacade,
                                   EdmlUploadFailedExecutor edmlUploadFailedExecutor,
                                   UploadExternalTableExecutor uploadExternalTableExecutor,
                                   @Qualifier("coreCalciteDefinitionService") DefinitionService<SqlNode> definitionService,
                                   @Value("${core.env.name}") String envName) {
        this.datamartDao = serviceDbFacade.getServiceDbDao().getDatamartDao();
        this.entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
        this.deltaServiceDao = serviceDbFacade.getDeltaServiceDao();
        this.edmlUploadFailedExecutor = edmlUploadFailedExecutor;
        this.uploadExternalTableExecutor = uploadExternalTableExecutor;
        this.definitionService = definitionService;
        this.envName = envName;
    }

    @Override
    public void restoreState(){
        datamartDao.getDatamarts()
                .compose(this::getOperations)
                .onSuccess(success -> log.info("State sucessfully restored"))
                .onFailure(err -> log.error("Error while trying to restore state", err));
    }

    private Future<Void> getOperations(List<String> datamarts) {
        return Future.future(p -> {
            CompositeFuture.join(datamarts.stream()
                    .map(this::getAndProcessOpertations)
                    .collect(Collectors.toList()))
                    .onSuccess(success -> p.complete())
                    .onFailure(p::fail);
        });
    }

    private Future<Void> getAndProcessOpertations(String datamart) {
        return deltaServiceDao.getDeltaWriteOperations(datamart)
                .compose(ops -> operationsProcess(datamart, ops));
    }

    private Future<Void> operationsProcess(String datamart, List<DeltaWriteOp> ops) {
        if (ops == null) {
            return Future.succeededFuture();
        }
        return Future.future(p -> {
            CompositeFuture.join(ops.stream()
                    .map(op -> getDestinationSourceEntities(datamart, op.getTableName(), op.getTableNameExt())
                            .compose(entities -> processWriteOperation(entities.get(0), entities.get(1), op)))
                    .collect(Collectors.toList()))
                    .onSuccess(success -> p.complete())
                    .onFailure(p::fail);
        });
    }

    private Future<List<Entity>> getDestinationSourceEntities(String datamart, String dest, String source) {
        return Future.future(p -> CompositeFuture.join(entityDao.getEntity(datamart, dest), entityDao.getEntity(datamart, source))
                .onSuccess(res -> p.complete(res.list()))
                .onFailure(p::fail));
    }

    private Future<Void> processWriteOperation(Entity dest, Entity source, DeltaWriteOp op) {
        Promise promise = Promise.promise();

        val queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setEnvName(envName);
        queryRequest.setSql(op.getQuery());
        val datamartRequest = new DatamartRequest(queryRequest);
        val sqlNode = definitionService.processingQuery(op.getQuery());
        val context = new EdmlRequestContext(datamartRequest, (SqlInsert) sqlNode);
        context.setSourceTable(new TableInfo(source.getSchema(), source.getName()));
        context.setDestinationTable(new TableInfo(dest.getSchema(), dest.getName()));
        context.setSysCn(op.getSysCn());
        context.setSourceEntity(source);
        context.setDestinationEntity(dest);

        if (op.getStatus() == 0) {
            uploadExternalTableExecutor.execute(context, promise);
            return promise.future();
        }
        else if (op.getStatus() == 2) {
            return edmlUploadFailedExecutor.execute(context);
        }
        else return Future.succeededFuture();
    }
}
