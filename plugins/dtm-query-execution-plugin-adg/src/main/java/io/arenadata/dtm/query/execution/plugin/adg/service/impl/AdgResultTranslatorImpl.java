package io.arenadata.dtm.query.execution.plugin.adg.service.impl;

import io.arenadata.dtm.query.execution.plugin.adg.exception.DtmTarantoolException;
import io.arenadata.dtm.query.execution.plugin.adg.service.AdgResultTranslator;
import lombok.val;
import org.tarantool.TarantoolException;

import java.util.List;
import java.util.Map;

public class AdgResultTranslatorImpl implements AdgResultTranslator {

    @Override
    public List<?> translate(List<?> list) {
        if (list.get(0) != null) {
            return list;
        }
        val error = list.get(1);
        if (error instanceof String) {
            throw new DtmTarantoolException((String) error);
        }
        if (error instanceof Map<?, ?>) {
            // TODO implement this
            // val stack = error["stack"] as String
            // val file = error["file"] as String
            // val className = error["class_name"] as String
            // val line = error["line"] as Int
            val str = (String) ((Map<?, ?>) error).get("str");
            val err = (String) ((Map<?, ?>) error).get("err");
            throw new DtmTarantoolException(0, err, new TarantoolException(0, str));
        } else {
            throw new DtmTarantoolException("Unknown type of error: " + error);
        }
    }
}
