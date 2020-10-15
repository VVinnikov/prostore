package ru.ibs.dtm.common.converter.transformer;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.ibs.dtm.common.model.ddl.ColumnType;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TimestampFromStringTransformer implements ColumnTransformer<Timestamp, String> {

    private DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    @Override
    public Timestamp transform(String value) {
        return value == null ? null : Timestamp.valueOf(LocalDateTime.parse(value, dateTimeFormatter));
    }

    @Override
    public Class<?> getTransformClass() {
        return String.class;
    }

    @Override
    public ColumnType getType() {
        return ColumnType.TIMESTAMP;
    }
}
