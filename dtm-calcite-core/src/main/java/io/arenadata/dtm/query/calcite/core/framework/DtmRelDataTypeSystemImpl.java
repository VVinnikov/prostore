package io.arenadata.dtm.query.calcite.core.framework;

import org.apache.calcite.rel.type.RelDataTypeSystemImpl;

public class DtmRelDataTypeSystemImpl extends RelDataTypeSystemImpl {

    public DtmRelDataTypeSystemImpl() {
    }

    @Override
    public boolean shouldConvertRaggedUnionTypesToVarying() {
        return true;
    }
}