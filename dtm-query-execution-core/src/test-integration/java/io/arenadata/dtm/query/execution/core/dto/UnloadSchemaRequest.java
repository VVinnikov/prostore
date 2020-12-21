package io.arenadata.dtm.query.execution.core.dto;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.arenadata.dtm.common.schema.SchemaDeserializer;
import io.arenadata.dtm.common.schema.SchemaSerializer;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.avro.Schema;

import java.util.UUID;

@Data
@NoArgsConstructor
public class UnloadSchemaRequest {
    private UUID requestId = UUID.randomUUID();
    private String kafkaTopic;
    private UnloadFormat format = UnloadFormat.AVRO;
    @JsonDeserialize(using = SchemaDeserializer.class)
    @JsonSerialize(using = SchemaSerializer.class)
    private Schema schema;
    private int from = 0;
    private int to = 1000;
    private int chunkSize = 1000;
}
