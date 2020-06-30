package ru.ibs.dtm.query.execution.core.dao;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.ext.sql.ResultSet;
import org.jooq.generated.dtmservice.tables.records.UploadExternalTableRecord;
import ru.ibs.dtm.common.dto.ActualDeltaRequest;
import ru.ibs.dtm.common.model.ddl.ClassField;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.query.execution.core.dto.delta.DeltaRecord;
import ru.ibs.dtm.query.execution.core.dto.eddl.CreateDownloadExternalTableQuery;
import ru.ibs.dtm.query.execution.core.dto.eddl.CreateUploadExternalTableQuery;
import ru.ibs.dtm.query.execution.core.dto.eddl.DropUploadExternalTableQuery;
import ru.ibs.dtm.query.execution.core.dto.edml.*;
import ru.ibs.dtm.query.execution.core.dto.metadata.DatamartEntity;
import ru.ibs.dtm.query.execution.core.dto.metadata.DatamartInfo;
import ru.ibs.dtm.query.execution.core.dto.metadata.EntityAttribute;

import java.util.List;
import java.util.UUID;

/**
 * Слой взаимодействия с сервисной БД
 */
public interface ServiceDao {

    void insertDatamart(String name, Handler<AsyncResult<Void>> resultHandler);

    void findDatamart(String name, Handler<AsyncResult<Long>> resultHandler);

    void dropDatamart(Long id, Handler<AsyncResult<Void>> resultHandler);

    void insertEntity(Long datamartId, String name, Handler<AsyncResult<Void>> resultHandler);

    void findEntity(Long datamartId, String name, Handler<AsyncResult<Long>> resultHandler);

    void dropEntity(Long datamartId, String name, Handler<AsyncResult<Void>> resultHandler);

    void insertAttribute(Long entityId, String name, Integer typeId, Integer length, Handler<AsyncResult<Void>> resultHandler);

    void dropAttribute(Long entityId, Handler<AsyncResult<Void>> resultHandler);

    void selectType(String name, Handler<AsyncResult<Integer>> resultHandler);

    void getDatamartMeta(Handler<AsyncResult<List<DatamartInfo>>> resultHandler);

    void getEntitiesMeta(String datamartMnemonic, Handler<AsyncResult<List<DatamartEntity>>> resultHandler);

    void getAttributesMeta(String datamartMnemonic, String entityMnemonic, Handler<AsyncResult<List<EntityAttribute>>> resultHandler);

    void getMetadataByTableName(String table, Handler<AsyncResult<List<ClassField>>> resultHandler);

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

    void getDeltaOnDateTime(ActualDeltaRequest actualDeltaRequest, Handler<AsyncResult<Long>> resultHandler);

    void getDeltasOnDateTimes(List<ActualDeltaRequest> actualDeltaRequests, Handler<AsyncResult<List<Long>>> resultHandler);

    void getDeltaHotByDatamart(String datamartMnemonic, Handler<AsyncResult<DeltaRecord>> resultHandler);

    void getDeltaActualBySinIdAndDatamart(String datamartMnemonic, Long sinId, Handler<AsyncResult<DeltaRecord>> resultHandler);

    void insertDelta(DeltaRecord delta, Handler<AsyncResult<Void>> resultHandler);

    void updateDelta(DeltaRecord delta, Handler<AsyncResult<Void>> resultHandler);

    void inserUploadQuery(UploadQueryRecord uploadQueryRecord, Handler<AsyncResult<Void>> resultHandler);
}
