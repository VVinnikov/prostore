package io.arenadata.dtm.common.converter.transformer;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TimestampFromLocalDateTimeTransformer implements ColumnTransformer<Timestamp, LocalDateTime> {

    private ZoneId zoneId = ZoneId.of("UTC");

    @Override
    public Timestamp transform(LocalDateTime value) {
        return value == null ? null : Timestamp.from(value.atZone(zoneId).toInstant());
    }

    @Override
    public Class<?> getTransformClass() {
        return LocalDateTime.class;
    }

    @Override
    public ColumnType getType() {
        return ColumnType.TIMESTAMP;
    }
}
