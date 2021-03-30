package io.arenadata.dtm.query.execution.plugin.adg.model.callback.function;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.arenadata.dtm.query.execution.plugin.adg.model.callback.params.TtKafkaCallbackParameter;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public abstract class TtKafkaCallbackFunction {

    @JsonProperty("callbackFunctionName")
    private String callbackFunctionName;
    @JsonProperty("callbackFunctionParams")
    private TtKafkaCallbackParameter callbackFunctionParams;
    @JsonProperty("maxNumberOfMessagesPerPartition")
    private Long callbackFunctionMsgCnt;
    @JsonProperty("maxIdleSecondsBeforeCbCall")
    private Long callbackFunctionSecCnt;

}
