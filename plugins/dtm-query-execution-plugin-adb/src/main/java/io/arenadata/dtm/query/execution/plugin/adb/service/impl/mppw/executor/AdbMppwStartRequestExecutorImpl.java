/*
 * Copyright © 2021 ProStore
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.executor;

import io.arenadata.dtm.common.dto.KafkaBrokerInfo;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.ExternalTableFormat;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adb.configuration.properties.MppwProperties;
import io.arenadata.dtm.query.execution.plugin.adb.factory.KafkaMppwSqlFactory;
import io.arenadata.dtm.query.execution.plugin.adb.factory.MppwKafkaLoadRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adb.factory.MppwTransferRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.MppwTopic;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwKafkaLoadRequest;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwKafkaRequestContext;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwTransferDataRequest;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
import io.arenadata.dtm.query.execution.plugin.api.exception.MppwDatasourceException;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Component("adbMppwStartRequestExecutor")
@Slf4j
public class AdbMppwStartRequestExecutorImpl implements AdbMppwRequestExecutor {

    private final AdbQueryExecutor adbQueryExecutor;
    private final KafkaMppwSqlFactory kafkaMppwSqlFactory;
    private final MppwTransferRequestFactory mppwTransferRequestFactory;
    private final MppwKafkaLoadRequestFactory mppwKafkaLoadRequestFactory;
    private final Vertx vertx;
    private final MppwProperties mppwProperties;
    private final String dbName;

    @Autowired
    public AdbMppwStartRequestExecutorImpl(AdbQueryExecutor adbQueryExecutor,
                                           KafkaMppwSqlFactory kafkaMppwSqlFactory,
                                           MppwTransferRequestFactory mppwTransferRequestFactory,
                                           MppwKafkaLoadRequestFactory mppwKafkaLoadRequestFactory,
                                           @Qualifier("coreVertx") Vertx vertx,
                                           MppwProperties mppwProperties,
                                           @Value("${core.env.name}") String dbName) {
        this.adbQueryExecutor = adbQueryExecutor;
        this.kafkaMppwSqlFactory = kafkaMppwSqlFactory;
        this.mppwTransferRequestFactory = mppwTransferRequestFactory;
        this.mppwKafkaLoadRequestFactory = mppwKafkaLoadRequestFactory;
        this.vertx = vertx;
        this.mppwProperties = mppwProperties;
        this.dbName = dbName;
    }

    @Override
    public Future<QueryResult> execute(MppwKafkaRequest request) {
        return Future.future((Promise<QueryResult> promise) -> {
            val format = request.getUploadMetadata().getFormat();
            if (!ExternalTableFormat.AVRO.equals(format)) {
                promise.fail(new MppwDatasourceException(String.format("Format %s not implemented", format)));
            }
            List<KafkaBrokerInfo> brokers = request.getBrokers();
            getOrCreateServer(brokers, dbName)
                    .compose(server -> createWritableExternalTable(request, server))
                    .compose(server -> createMppwKafkaRequestContext(request, server))
                    .compose(kafkaContext -> moveOffsetsExtTable(request).map(v -> kafkaContext))
                    .onSuccess(kafkaContext -> {
                        vertx.eventBus().send(MppwTopic.KAFKA_START.getValue(), Json.encode(kafkaContext));
                        log.debug("Mppw started successfully");
                        promise.complete(QueryResult.emptyResult());
                    })
                    .onFailure(promise::fail);
        });
    }

    private Future<String> getOrCreateServer(List<KafkaBrokerInfo> brokers, String currentDatabase) {
        return Future.future(promise -> {
            val columnMetadata = Collections.singletonList(
                    new ColumnMetadata("foreign_server_name", ColumnType.VARCHAR));
            val brokersList = brokers.stream()
                    .map(KafkaBrokerInfo::getAddress)
                    .collect(Collectors.joining(","));
            final String serverSqlQuery = kafkaMppwSqlFactory.checkServerSqlQuery(currentDatabase, brokersList);
            log.debug("Created check server for mppw query {}", serverSqlQuery);
            adbQueryExecutor.execute(serverSqlQuery, columnMetadata)
                    .compose(result -> {
                        if (result.isEmpty()) {
                            return createServer(brokersList, currentDatabase);
                        } else {
                            return Future.succeededFuture(result.get(0).get("foreign_server_name").toString());
                        }
                    })
                    .onComplete(promise);
        });
    }

    private Future<String> createServer(String brokersList, String currentDatabase) {
        return adbQueryExecutor.execute(kafkaMppwSqlFactory.createServerSqlQuery(currentDatabase, brokersList), Collections.emptyList())
                .map(v -> kafkaMppwSqlFactory.getServerName(currentDatabase));
    }

    private Future<String> createWritableExternalTable(MppwKafkaRequest request, String server) {
        return Future.future(promise -> {
            val sourceEntity = request.getSourceEntity();
            val columns = kafkaMppwSqlFactory.getColumnsFromEntity(sourceEntity);
            columns.add("sys_op int");
            adbQueryExecutor.executeUpdate(kafkaMppwSqlFactory.createExtTableSqlQuery(server,
                    columns,
                    request,
                    mppwProperties))
                    .onSuccess(success -> promise.complete(server))
                    .onFailure(promise::fail);
        });
    }

    private Future<Void> moveOffsetsExtTable(MppwKafkaRequest request) {
        val schema = request.getDatamartMnemonic();
        val table = kafkaMppwSqlFactory.getTableName(request.getRequestId().toString());
        return adbQueryExecutor.executeUpdate(kafkaMppwSqlFactory.insertIntoKadbOffsetsSqlQuery(schema, table))
                .compose(v -> adbQueryExecutor.executeUpdate(kafkaMppwSqlFactory.moveOffsetsExtTableSqlQuery(schema, table)));
    }

    private Future<MppwKafkaRequestContext> createMppwKafkaRequestContext(MppwKafkaRequest request, String server) {
        return Future.future((Promise<MppwKafkaRequestContext> promise) -> {
            final MppwKafkaLoadRequest mppwKafkaLoadRequest =
                    mppwKafkaLoadRequestFactory.create(request, server, mppwProperties);
            final String keyColumnsSqlQuery = kafkaMppwSqlFactory.createKeyColumnsSqlQuery(
                    request.getDatamartMnemonic(),
                    request.getDestinationTableName());
            final List<ColumnMetadata> metadata = kafkaMppwSqlFactory.createKeyColumnQueryMetadata();
            adbQueryExecutor.execute(keyColumnsSqlQuery, metadata)
                    .onSuccess(result -> {
                        final MppwTransferDataRequest mppwTransferDataRequest =
                                mppwTransferRequestFactory.create(request, result);
                        MppwKafkaRequestContext kafkaRequestContext =
                                new MppwKafkaRequestContext(mppwKafkaLoadRequest, mppwTransferDataRequest);
                        promise.complete(kafkaRequestContext);
                    })
                    .onFailure(promise::fail);
        });
    }
}
