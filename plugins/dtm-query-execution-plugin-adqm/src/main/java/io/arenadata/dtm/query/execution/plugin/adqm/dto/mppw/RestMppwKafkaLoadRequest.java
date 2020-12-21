package io.arenadata.dtm.query.execution.plugin.adqm.dto.mppw;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.arenadata.dtm.common.dto.KafkaBrokerInfo;
import io.arenadata.dtm.common.schema.SchemaDeserializer;
import io.arenadata.dtm.common.schema.SchemaSerializer;
import lombok.Builder;
import lombok.Data;
import org.apache.avro.Schema;

import java.io.Serializable;
import java.util.List;

@Data
@Builder
public class RestMppwKafkaLoadRequest implements Serializable {
    private String requestId;
    private long hotDelta;
    private String datamart;
    private String tableName;
    private List<KafkaBrokerInfo> kafkaBrokers;
    private String kafkaTopic;
    private String consumerGroup;
    private String format;
    @JsonDeserialize(using = SchemaDeserializer.class)
    @JsonSerialize(using = SchemaSerializer.class)
    private Schema schema;
    private int messageProcessingLimit;
}