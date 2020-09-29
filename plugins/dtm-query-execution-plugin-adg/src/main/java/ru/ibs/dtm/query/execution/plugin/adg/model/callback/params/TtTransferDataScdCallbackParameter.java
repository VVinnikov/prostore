package ru.ibs.dtm.query.execution.plugin.adg.model.callback.params;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TtTransferDataScdCallbackParameter implements TtKafkaCallbackParameter {

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
