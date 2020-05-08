package ru.ibs.dtm.query.execution.plugin.adg.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.OperationFile;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.response.ResOperation;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.response.ResStatus;

import java.util.List;

/**
 * REST-клиент общения с Tarantool Cartridge
 */
public interface TtCartridgeClient {
  void getFiles(Handler<AsyncResult<ResOperation>> handler);

  void setFiles(List<OperationFile> files, Handler<AsyncResult<ResOperation>> handler);

  void getSchema(Handler<AsyncResult<ResOperation>> handler);

  void setSchema(String yaml, Handler<AsyncResult<ResOperation>> handler);

  void uploadData(String sql, String topicName, int batchSize, Handler<AsyncResult<ResStatus>> handler);
}
