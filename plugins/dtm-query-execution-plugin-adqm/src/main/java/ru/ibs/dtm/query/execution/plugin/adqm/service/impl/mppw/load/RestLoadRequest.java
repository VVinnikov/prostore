package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.mppw.load;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Data;
import org.apache.avro.Schema;
import ru.ibs.dtm.common.schema.SchemaDeserializer;
import ru.ibs.dtm.common.schema.SchemaSerializer;

import java.io.Serializable;

@Data
public class RestLoadRequest implements Serializable {
    private String requestId;
    private long synCn;
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
