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
 * REST-client for connecting with Tarantool Cartridge
 */
public interface AdgCartridgeClient {

  Future<ResOperation> getFiles();

  Future<ResOperation> setFiles(List<OperationFile> files);

  Future<ResOperation> getSchema();

  Future<ResOperation> setSchema(String yaml);

  Future<Void> uploadData(TtUploadDataKafkaRequest request);

  Future<Void> subscribe(TtSubscriptionKafkaRequest request);

  Future<TtLoadDataKafkaResponse> loadData(TtLoadDataKafkaRequest request);

  Future<Void> transferDataToScdTable(TtTransferDataEtlRequest request);

  Future<Void> cancelSubscription(String topicName);

  Future<TtDeleteBatchResponse> addSpacesToDeleteQueue(TtDeleteTablesRequest request);

  Future<TtDeleteQueueResponse> executeDeleteQueue(TtDeleteTablesQueueRequest request);

  Future<TtDeleteQueueResponse> executeDeleteSpacesWithPrefix(TtDeleteTablesWithPrefixRequest request);

  Future<Void> reverseHistoryTransfer(ReverseHistoryTransferRequest request);

  Future<Void> executeCreateSpacesQueued(OperationYaml request);

  Future<Void> executeDeleteSpacesQueued(TtDeleteTablesRequest request);

  Future<Void> executeDeleteSpacesWithPrefixQueued(TtDeleteTablesWithPrefixRequest request);

  Future<Map<String, Space>> getSpaceDescriptions(Set<String> spaceNames);

  Future<Long> getCheckSumByInt32Hash(String actualDataTableName,
                                        String historicalDataTableName,
                                        Long sysCn,
                                        Set<String> columnList);

  Future<Void> deleteSpaceTuples(String spaceName, String whereCondition);
}
