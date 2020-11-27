package io.arenadata.dtm.query.execution.plugin.adg.service;

import io.arenadata.dtm.query.execution.plugin.adg.dto.rollback.ReverseHistoryTransferRequest;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.OperationFile;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.OperationYaml;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.request.*;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.response.ResOperation;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.response.TtDeleteBatchResponse;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.response.TtDeleteQueueResponse;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.response.TtLoadDataKafkaResponse;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.schema.Space;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * REST-клиент общения с Tarantool Cartridge
 */
public interface AdgCartridgeClient {
  void getFiles(Handler<AsyncResult<ResOperation>> handler);

  void setFiles(List<OperationFile> files, Handler<AsyncResult<ResOperation>> handler);

  void getSchema(Handler<AsyncResult<ResOperation>> handler);

  void setSchema(String yaml, Handler<AsyncResult<ResOperation>> handler);

  void uploadData(TtUploadDataKafkaRequest request, Handler<AsyncResult<Void>> handler);

  void subscribe(TtSubscriptionKafkaRequest request, Handler<AsyncResult<Void>> handler);

  void loadData(TtLoadDataKafkaRequest request,
                Handler<AsyncResult<TtLoadDataKafkaResponse>> handler);

  void transferDataToScdTable(TtTransferDataEtlRequest request,
                              Handler<AsyncResult<Void>> handler);

  void cancelSubscription(String topicName, Handler<AsyncResult<Void>> handler);

  void addSpacesToDeleteQueue(TtDeleteTablesRequest request, Handler<AsyncResult<TtDeleteBatchResponse>> handler);

  void executeDeleteQueue(TtDeleteTablesQueueRequest request, Handler<AsyncResult<TtDeleteQueueResponse>> handler);

  void executeDeleteSpacesWithPrefix(TtDeleteTablesWithPrefixRequest request,
                                    Handler<AsyncResult<TtDeleteQueueResponse>> handler);

  void reverseHistoryTransfer(ReverseHistoryTransferRequest request, Handler<AsyncResult<Void>> handler);

  void executeCreateSpacesQueued(OperationYaml request, Handler<AsyncResult<Void>> handler);

  void executeDeleteSpacesQueued(TtDeleteTablesRequest request, Handler<AsyncResult<Void>> handler);

  void executeDeleteSpacesWithPrefixQueued(TtDeleteTablesWithPrefixRequest request,
                                           Handler<AsyncResult<Void>> handler);

  Future<Map<String, Space>> getSpaceDescriptions(Set<String> spaceNames);
}
