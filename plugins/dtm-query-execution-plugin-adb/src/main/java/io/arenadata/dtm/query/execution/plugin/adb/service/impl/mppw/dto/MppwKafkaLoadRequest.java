package io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.arenadata.dtm.common.dto.KafkaBrokerInfo;
import io.arenadata.dtm.common.schema.SchemaDeserializer;
import io.arenadata.dtm.common.schema.SchemaSerializer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.avro.Schema;

import java.io.Serializable;
import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MppwKafkaLoadRequest implements Serializable {
    private String requestId;
    private String datamart;
    private String tableName;
    private List<String> columns;
    private String writableExtTableName;
    @JsonDeserialize(using = SchemaDeserializer.class)
    @JsonSerialize(using = SchemaSerializer.class)
    private Schema schema;
    private String brokers;
    private String server;
    private String topic;
    private String consumerGroup;
    Long uploadMessageLimit;
    Long timeout;
}
