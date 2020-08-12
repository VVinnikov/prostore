package ru.ibs.dtm.query.execution.plugin.adg.model.callback.params;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.vertx.core.json.JsonObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TtTransferDataScdCallbackParameter extends TtKafkaCallbackParameter {

    @JsonProperty("_space")
    private String spaceName;
    @JsonProperty("_stage_data_table_name")
    private String stageTableName;
    @JsonProperty("_actual_data_table_name")
    private String actualTableName;
    @JsonProperty("_historical_data_table_name")
    private String historyTableName;
    @JsonProperty("_delta_number")
    private long deltaNumber;

}
