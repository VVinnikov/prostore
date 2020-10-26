package io.arenadata.dtm.query.execution.plugin.adg.model.callback.function;

import io.arenadata.dtm.query.execution.plugin.adg.model.callback.params.TtTransferDataScdCallbackParameter;

public class TtTransferDataScdCallbackFunction extends TtKafkaCallbackFunction {

    public TtTransferDataScdCallbackFunction(
            String callbackFunctionName,
            TtTransferDataScdCallbackParameter callbackFunctionParams,
            Long callbackFunctionMsgCnt,
            Long callbackFunctionSecCnt) {
        super(callbackFunctionName, callbackFunctionParams, callbackFunctionMsgCnt, callbackFunctionSecCnt);
    }


}
