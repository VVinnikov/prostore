package io.arenadata.dtm.query.execution.core.factory.impl;

import io.arenadata.dtm.common.dto.KafkaBrokerInfo;
import io.arenadata.dtm.common.plugin.exload.Format;
import io.arenadata.dtm.kafka.core.configuration.kafka.KafkaZookeeperProperties;
import io.arenadata.dtm.kafka.core.service.kafka.KafkaZookeeperConnectionProvider;
import io.arenadata.dtm.kafka.core.service.kafka.KafkaZookeeperConnectionProviderImpl;
import io.arenadata.dtm.query.execution.core.factory.MppwKafkaRequestFactory;
import io.arenadata.dtm.query.execution.core.service.zookeeper.ZookeeperConnectionProvider;
import io.arenadata.dtm.query.execution.core.utils.LocationUriParser;
import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaParameter;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.UploadExternalEntityMetadata;
import io.arenadata.dtm.query.execution.plugin.api.request.MppwRequest;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class MppwKafkaRequestFactoryImpl implements MppwKafkaRequestFactory {

    private final Map<String, KafkaZookeeperConnectionProvider> zkConnProviderMap;

    @Autowired
    public MppwKafkaRequestFactoryImpl(@Qualifier("coreKafkaZkConnProviderMap")
                                               Map<String, KafkaZookeeperConnectionProvider> zkConnProviderMap) {
        this.zkConnProviderMap = zkConnProviderMap;
    }

    @Override
    public MppwRequestContext create(EdmlRequestContext context) {
        LocationUriParser.KafkaTopicUri kafkaTopicUri =
                LocationUriParser.parseKafkaLocationPath(context.getSourceEntity().getExternalTableLocationPath());
        val request = MppwRequest.builder()
                .queryRequest(context.getRequest().getQueryRequest())
                .isLoadStart(true)
                .kafkaParameter(MppwKafkaParameter.builder()
                        .datamart(context.getSourceEntity().getSchema())
                        .sysCn(context.getSysCn())
                        .targetTableName(context.getDestinationEntity().getName())
                        .uploadMetadata(UploadExternalEntityMetadata.builder()
                                .name(context.getSourceEntity().getName())
                                .format(Format.findByName(context.getSourceEntity().getExternalTableFormat()))
                                .locationPath(context.getSourceEntity().getExternalTableLocationPath())
                                .externalSchema(context.getSourceEntity().getExternalTableSchema())
                                .uploadMessageLimit(context.getSourceEntity().getExternalTableUploadMessageLimit())
                                .build())
                        .brokers(getKafkaBrokerList(kafkaTopicUri.getAddress()))
                        .topic(kafkaTopicUri.getTopic())
                        .build())
                .build();
        return new MppwRequestContext(request);
    }

    private List<KafkaBrokerInfo> getKafkaBrokerList(String zkConnectionString) {
        final KafkaZookeeperConnectionProvider zkConnProvider = zkConnProviderMap.get(zkConnectionString);
        final KafkaZookeeperProperties zookeeperProperties = new KafkaZookeeperProperties();
        zookeeperProperties.setConnectionString(zkConnectionString);
        if (zkConnProvider == null) {
            zkConnProviderMap.put(zookeeperProperties.getConnectionString(),
                    new KafkaZookeeperConnectionProviderImpl(zookeeperProperties));
        }
        return ((KafkaZookeeperConnectionProviderImpl)
                zkConnProviderMap.get(zookeeperProperties.getConnectionString())).getKafkaBrokers();
    }
}
