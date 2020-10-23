package io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.arenadata.dtm.common.schema.SchemaDeserializer;
import io.arenadata.dtm.common.schema.SchemaSerializer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.avro.Schema;

import java.io.Serializable;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class RestLoadRequest implements Serializable {
    private String requestId;
    private long hotDelta;
    private String datamart;
    private String tableName;
    private String zookeeperHost;
    private int zookeeperPort;
    private String kafkaTopic;
    private String consumerGroup;
    private String format;
    @JsonDeserialize(using = SchemaDeserializer.class)
    @JsonSerialize(using = SchemaSerializer.class)
    private Schema schema;
    private int messageProcessingLimit;

}
