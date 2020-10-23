package io.arenadata.dtm.common.converter.transformer;

import io.arenadata.dtm.common.model.ddl.ColumnType;

import java.math.BigInteger;

public class BigintFromBigIntegerTransformer implements ColumnTransformer<Long, BigInteger> {

    @Override
    public Long transform(BigInteger value) {
        return value.longValue();
    }

    @Override
    public Class<?> getTransformClass() {
        return BigInteger.class;
    }

    @Override
    public ColumnType getType() {
        return ColumnType.BIGINT;
    }
}
