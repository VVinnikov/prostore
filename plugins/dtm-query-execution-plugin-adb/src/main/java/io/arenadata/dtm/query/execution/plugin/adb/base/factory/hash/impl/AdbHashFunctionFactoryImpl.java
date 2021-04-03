package io.arenadata.dtm.query.execution.plugin.adb.base.factory.hash.impl;

import io.arenadata.dtm.query.execution.plugin.adb.base.factory.hash.AdbHashFunctionFactory;
import org.springframework.stereotype.Service;

@Service
public class AdbHashFunctionFactoryImpl implements AdbHashFunctionFactory {

    private static final String CREATE_OR_REPLACE_FUNC = "CREATE OR REPLACE FUNCTION dtmInt32Hash(bytea) RETURNS integer\n" +
            "    AS 'select get_byte($1, 0)+(get_byte($1, 1)<<8)+(get_byte($1, 2)<<16)+(get_byte($1, 3)<<24)' \n" +
            "    LANGUAGE SQL\n" +
            "    IMMUTABLE\n" +
            "    LEAKPROOF\n" +
            "    RETURNS NULL ON NULL INPUT;";

    @Override
    public String createInt32HashFunction() {
        return CREATE_OR_REPLACE_FUNC;
    }
}
