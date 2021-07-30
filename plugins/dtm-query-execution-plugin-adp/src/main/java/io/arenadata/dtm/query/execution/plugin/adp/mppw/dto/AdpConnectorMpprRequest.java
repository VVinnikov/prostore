package io.arenadata.dtm.query.execution.plugin.adp.mppw.dto;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.arenadata.dtm.common.dto.KafkaBrokerInfo;
import io.arenadata.dtm.common.schema.SchemaDeserializer;
import io.arenadata.dtm.common.schema.SchemaSerializer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.avro.Schema;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AdpConnectorMpprRequest {
    private String requestId;
    private String table;
    private String datamart;
    private String sql;
    private List<KafkaBrokerInfo> kafkaBrokers;
    private String kafkaTopic;
    private int chunkSize = 1000;
    @JsonDeserialize(using = SchemaDeserializer.class)
    @JsonSerialize(using = SchemaSerializer.class)
    private Schema avroSchema;
}
