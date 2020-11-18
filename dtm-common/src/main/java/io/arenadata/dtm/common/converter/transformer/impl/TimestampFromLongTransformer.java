package io.arenadata.dtm.common.converter.transformer.impl;

import io.arenadata.dtm.common.converter.transformer.AbstractColumnTransformer;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;

@Data
@NoArgsConstructor
public class TimestampFromLongTransformer extends AbstractColumnTransformer<Timestamp, Long> {

    @Override
    public Timestamp transformValue(Long value) {
        return value == null ? null : Timestamp.from(Instant.ofEpochMilli(value / 1000));
    }

    @Override
    public Collection<Class<?>> getTransformClasses() {
        return Collections.singletonList(Long.class);
    }

    @Override
    public ColumnType getType() {
        return ColumnType.TIMESTAMP;
    }
}
