package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.mppw.load;

import lombok.NonNull;
import org.apache.avro.Schema;
import ru.ibs.dtm.query.execution.plugin.adqm.common.DdlUtils;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.properties.DdlProperties;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.properties.MppwProperties;

import java.util.stream.Collectors;

import static java.lang.String.format;
import static ru.ibs.dtm.query.execution.plugin.adqm.common.Constants.EXT_SHARD_POSTFIX;

public class KafkaExtTableCreator implements ExtTableCreator {
    private static final String KAFKA_ENGINE_TEMPLATE = "ENGINE = Kafka()\n" +
            "  SETTINGS\n" +
            "    kafka_broker_list = '%s',\n" +
            "    kafka_topic_list = '%s',\n" +
            "    kafka_group_name = '%s',\n" +
            "    kafka_format = '%s'";
    private static final String EXT_SHARD_TEMPLATE =
            "CREATE TABLE IF NOT EXISTS %s ON CLUSTER %s (\n" +
                    "  %s\n" +
                    ")\n" +
                    "%s\n";

    private final DdlProperties ddlProperties;
    private final MppwProperties mppwProperties;

    public KafkaExtTableCreator(DdlProperties ddlProperties, MppwProperties mppwProperties) {
        this.ddlProperties = ddlProperties;
        this.mppwProperties = mppwProperties;
    }

    @Override
    public String generate(@NonNull String topic, @NonNull String table, @NonNull Schema schema, @NonNull String sortingKey) {
        String kafkaSettings = genKafkaEngine(topic, table);

        String columns = schema.getFields().stream()
                .map(DdlUtils::avroFieldToString)
                .collect(Collectors.joining(", "));
        return format(EXT_SHARD_TEMPLATE, table + EXT_SHARD_POSTFIX, ddlProperties.getCluster(), columns, kafkaSettings);
    }

    private String genKafkaEngine(@NonNull String topic, @NonNull String tableName) {
        String brokers = mppwProperties.getKafkaBrokers();
        String consumerGroup = getConsumerGroupName(tableName);
        // FIXME Support other formats (Text, CSV, Json?)
        String format = "Avro";
        return format(KAFKA_ENGINE_TEMPLATE, brokers, topic, consumerGroup, format);
    }

    @NonNull
    private String getConsumerGroupName(@NonNull String tableName) {
        return mppwProperties.getConsumerGroup() + tableName;
    }
}
