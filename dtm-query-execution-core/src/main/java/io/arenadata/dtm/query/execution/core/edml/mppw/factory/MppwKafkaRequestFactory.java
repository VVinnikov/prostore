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
package io.arenadata.dtm.query.execution.core.edml.mppw.factory;

import io.arenadata.dtm.common.dto.KafkaBrokerInfo;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.kafka.core.repository.ZookeeperKafkaProviderRepository;
import io.arenadata.dtm.query.execution.core.edml.dto.EdmlRequestContext;
import io.arenadata.dtm.query.execution.core.edml.exception.UnreachableLocationException;
import io.arenadata.dtm.query.execution.core.edml.service.LocationUriParser;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.UploadExternalEntityMetadata;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class MppwKafkaRequestFactory {

    private final ZookeeperKafkaProviderRepository zkConnProviderRepository;
    private final LocationUriParser locationUriParser;
    private final Vertx vertx;

    @Autowired
    public MppwKafkaRequestFactory(@Qualifier("mapZkKafkaProviderRepository") ZookeeperKafkaProviderRepository zkConnProviderRepository,
                                   LocationUriParser locationUriParser,
                                   @Qualifier("coreVertx") Vertx vertx) {
        this.zkConnProviderRepository = zkConnProviderRepository;
        this.locationUriParser = locationUriParser;
        this.vertx = vertx;
    }

    public Future<MppwKafkaRequest> create(EdmlRequestContext context) {
        return Future.future(p -> {
            LocationUriParser.KafkaTopicUri kafkaTopicUri =
                    locationUriParser.parseKafkaLocationPath(context.getSourceEntity().getExternalTableLocationPath());
            getBrokers(kafkaTopicUri.getAddress())
                    .map(kafkaBrokers -> MppwKafkaRequest.builder()
                            .requestId(context.getRequest().getQueryRequest().getRequestId())
                            .envName(context.getEnvName())
                            .datamartMnemonic(context.getRequest().getQueryRequest().getDatamartMnemonic())
                            .isLoadStart(true)
                            .sysCn(context.getSysCn())
                            .destinationTableName(context.getDestinationEntity().getName())
                            .sourceEntity(context.getSourceEntity())
                            .brokers(kafkaBrokers)
                            .topic(kafkaTopicUri.getTopic())
                            .primaryKeys(context.getDestinationEntity().getFields().stream()
                                    .filter(field -> field.getPrimaryOrder() != null)
                                    .sorted(Comparator.comparingInt(EntityField::getPrimaryOrder))
                                    .map(EntityField::getName)
                                    .collect(Collectors.toList()))
                            .uploadMetadata(UploadExternalEntityMetadata.builder()
                                    .name(context.getSourceEntity().getName())
                                    .format(context.getSourceEntity().getExternalTableFormat())
                                    .locationPath(context.getSourceEntity().getExternalTableLocationPath())
                                    .externalSchema(context.getSourceEntity().getExternalTableSchema())
                                    .uploadMessageLimit(context.getSourceEntity().getExternalTableUploadMessageLimit())
                                    .build())
                            .build())
                    .onComplete(p);
        });
    }

    private Future<List<KafkaBrokerInfo>> getBrokers(String connectionString) {
        return vertx.executeBlocking(p -> {
            try {
                val kafkaBrokers = zkConnProviderRepository.getOrCreate(connectionString).getKafkaBrokers();
                p.complete(kafkaBrokers);
            } catch (Exception e) {
                p.fail(new UnreachableLocationException(connectionString, e));
            }
        }, false);
    }
}
