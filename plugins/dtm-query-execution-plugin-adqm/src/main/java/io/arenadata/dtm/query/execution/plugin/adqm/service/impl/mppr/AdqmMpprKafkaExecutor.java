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
package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.mppr;

import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adqm.factory.MpprKafkaConnectorRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adqm.service.AdqmMpprExecutor;
import io.arenadata.dtm.query.execution.plugin.adqm.service.MpprKafkaConnectorService;
import io.arenadata.dtm.query.execution.plugin.adqm.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppr.kafka.MpprKafkaRequest;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service("adqmMpprKafkaService")
@Slf4j
public class AdqmMpprKafkaExecutor implements AdqmMpprExecutor {

    private final QueryEnrichmentService adqmQueryEnrichmentService;
    private final MpprKafkaConnectorService mpprKafkaConnectorService;
    private final MpprKafkaConnectorRequestFactory requestFactory;

    @Autowired
    public AdqmMpprKafkaExecutor(@Qualifier("adqmQueryEnrichmentService") QueryEnrichmentService queryEnrichmentService,
                                 MpprKafkaConnectorService mpprKafkaConnectorService,
                                 MpprKafkaConnectorRequestFactory requestFactory) {
        this.adqmQueryEnrichmentService = queryEnrichmentService;
        this.mpprKafkaConnectorService = mpprKafkaConnectorService;
        this.requestFactory = requestFactory;
    }

    @Override
    public Future<QueryResult> execute(MpprRequest request) {
        val kafkaRequest = (MpprKafkaRequest) request;
        return adqmQueryEnrichmentService.enrich(EnrichQueryRequest.builder()
                .query(kafkaRequest.getDmlSubQuery())
                .deltaInformations(kafkaRequest.getDeltaInformations())
                .envName(kafkaRequest.getEnvName())
                .schema(kafkaRequest.getLogicalSchema())
                .build())
                .compose(enrichedQuery -> mpprKafkaConnectorService.call(
                        requestFactory.create(kafkaRequest, enrichedQuery)));
    }

    @Override
    public ExternalTableLocationType getType() {
        return ExternalTableLocationType.KAFKA;
    }
}
