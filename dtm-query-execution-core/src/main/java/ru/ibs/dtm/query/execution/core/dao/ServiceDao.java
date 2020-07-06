package ru.ibs.dtm.query.execution.core.dao;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.sql.ResultSet;
import ru.ibs.dtm.common.dto.ActualDeltaRequest;
import ru.ibs.dtm.common.model.ddl.ClassField;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.query.execution.core.dto.DatamartView;
import ru.ibs.dtm.query.execution.core.dto.delta.DeltaRecord;
import ru.ibs.dtm.query.execution.core.dto.eddl.CreateDownloadExternalTableQuery;
import ru.ibs.dtm.query.execution.core.dto.eddl.CreateUploadExternalTableQuery;
import ru.ibs.dtm.query.execution.core.dto.eddl.DropUploadExternalTableQuery;
import ru.ibs.dtm.query.execution.core.dto.edml.*;
import ru.ibs.dtm.query.execution.core.dto.metadata.DatamartEntity;
import ru.ibs.dtm.query.execution.core.dto.metadata.DatamartInfo;
import ru.ibs.dtm.query.execution.core.dto.metadata.EntityAttribute;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;

import java.util.List;

/**
 * Слой взаимодействия с сервисной БД
 */
public interface ServiceDao {

    void insertDatamart(String name, Handler<AsyncResult<Void>> resultHandler);

    void findDatamart(String name, Handler<AsyncResult<Long>> resultHandler);

    void dropDatamart(Long id, Handler<AsyncResult<Void>> resultHandler);

    void insertEntity(Long datamartId, String name, Handler<AsyncResult<Void>> resultHandler);

    void findEntity(Long datamartId, String name, Handler<AsyncResult<Long>> resultHandler);

    void existsEntity(Long datamartId, String name, Handler<AsyncResult<Boolean>> resultHandler);

    Future<Integer> dropEntity(Long datamartId, String name);

    void insertAttribute(Long entityId, ClassField field, Integer typeId, Handler<AsyncResult<Void>> resultHandler);

    void dropAttribute(Long entityId, Handler<AsyncResult<Void>> resultHandler);

    void selectType(String name, Handler<AsyncResult<Integer>> resultHandler);

    void getDatamartMeta(Handler<AsyncResult<List<DatamartInfo>>> resultHandler);

    void getEntitiesMeta(String datamartMnemonic, Handler<AsyncResult<List<DatamartEntity>>> resultHandler);

    void getAttributesMeta(String datamartMnemonic, String entityMnemonic, Handler<AsyncResult<List<EntityAttribute>>> resultHandler);

    void getMetadataByTableName(DdlRequestContext context, String table, Handler<AsyncResult<List<ClassField>>> resultHandler);

    void executeUpdate(String sql, Handler<AsyncResult<List<Void>>> resultHandler);

    void executeQuery(String sql, Handler<AsyncResult<ResultSet>> resultHandler);

    void dropTable(ClassTable classTable, Handler<AsyncResult<Void>> resultHandler);

    void insertDownloadExternalTable(CreateDownloadExternalTableQuery downloadExternalTableQuery, Handler<AsyncResult<Void>> resultHandler);

    void findDownloadExternalTableAttributes(Long detId, Handler<AsyncResult<List<DownloadExternalTableAttribute>>> resultHandler);

    void dropDownloadExternalTable(String datamart, String tableName, Handler<AsyncResult<Void>> resultHandler);

    void findDownloadExternalTable(String datamartMnemonic, String table, Handler<AsyncResult<DownloadExtTableRecord>> resultHandler);

    void insertDownloadQuery(DownloadQueryRecord downloadQueryRecord, Handler<AsyncResult<Void>> resultHandler);

    void findUploadExternalTable(String schemaName, String tableName, Handler<AsyncResult<UploadExtTableRecord>> resultHandler);

    void insertUploadExternalTable(CreateUploadExternalTableQuery query, Handler<AsyncResult<Void>> asyncResultHandler);

    void dropUploadExternalTable(DropUploadExternalTableQuery query, Handler<AsyncResult<Void>> asyncResultHandler);

    void inserUploadQuery(UploadQueryRecord uploadQueryRecord, Handler<AsyncResult<Void>> resultHandler);

    void getDeltaOnDateTime(ActualDeltaRequest actualDeltaRequest, Handler<AsyncResult<Long>> resultHandler);

    void getDeltasOnDateTimes(List<ActualDeltaRequest> actualDeltaRequests, Handler<AsyncResult<List<Long>>> resultHandler);

    void getDeltaHotByDatamart(String datamartMnemonic, Handler<AsyncResult<DeltaRecord>> resultHandler);

    void getDeltaActualBySinIdAndDatamart(String datamartMnemonic, Long sinId, Handler<AsyncResult<DeltaRecord>> resultHandler);

    void insertDelta(DeltaRecord delta, Handler<AsyncResult<Void>> resultHandler);

    void updateDelta(DeltaRecord delta, Handler<AsyncResult<Void>> resultHandler);

    void existsView(String viewName, Long datamartId, Handler<AsyncResult<Boolean>> resultHandler);

    void findViewsByDatamart(String datamart, List<String> views, Handler<AsyncResult<List<DatamartView>>> resultHandler);

    void insertView(String viewName, Long datamartId, String query, Handler<AsyncResult<Void>> resultHandler);

    void updateView(String viewName, Long datamartId, String query, Handler<AsyncResult<Void>> resultHandler);

    void dropView(String viewName, Long datamartId, Handler<AsyncResult<Void>> resultHandler);
}
