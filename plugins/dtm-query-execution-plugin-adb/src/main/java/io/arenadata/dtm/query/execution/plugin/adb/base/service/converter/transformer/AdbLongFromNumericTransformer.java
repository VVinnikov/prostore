package io.arenadata.dtm.query.execution.plugin.adb.base.service.converter.transformer;

import io.arenadata.dtm.common.converter.transformer.impl.LongFromNumberTransformer;
import io.reactiverse.pgclient.data.Numeric;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;

public class AdbLongFromNumericTransformer extends LongFromNumberTransformer {
    @Override
    public Collection<Class<?>> getTransformClasses() {
        return Arrays.asList(Long.class, Integer.class, BigInteger.class, Numeric.class);
    }
}
