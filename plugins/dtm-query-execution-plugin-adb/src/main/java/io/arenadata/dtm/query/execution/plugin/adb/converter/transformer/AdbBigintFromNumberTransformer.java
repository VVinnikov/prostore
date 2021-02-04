package io.arenadata.dtm.query.execution.plugin.adb.converter.transformer;

import io.arenadata.dtm.common.converter.transformer.impl.BigintFromNumberTransformer;
import io.reactiverse.pgclient.data.Numeric;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;

public class AdbBigintFromNumberTransformer extends BigintFromNumberTransformer {
    @Override
    public Collection<Class<?>> getTransformClasses() {
        return Arrays.asList(Long.class, Integer.class, BigInteger.class, Numeric.class);
    }
}
