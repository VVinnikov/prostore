package ru.ibs.dtm.common.converter.transformer;

import ru.ibs.dtm.common.model.ddl.ColumnType;

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
