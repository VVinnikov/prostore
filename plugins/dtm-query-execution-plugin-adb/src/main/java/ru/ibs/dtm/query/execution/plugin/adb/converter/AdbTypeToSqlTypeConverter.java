package ru.ibs.dtm.query.execution.plugin.adb.converter;

import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.converter.BaseSqlTypeConverter;
import ru.ibs.dtm.common.model.ddl.ColumnType;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;

@Component("adbTypeToSqlTypeConverter")
public class AdbTypeToSqlTypeConverter extends BaseSqlTypeConverter {

    @Override
    public Object convert(Object value) {
        if (value.getClass().equals(LocalTime.class)) {
            //made for the same display of the LocalTime type as Long value in all plugins,
            //since Calcite does not support the Time type
            return this.convert(((LocalTime) value).toNanoOfDay());
        }
        return value;
    }

    @Override
    public Object convert(ColumnType type, Object value) {
        switch (type) {
            case INT:
                return this.convert((Integer) value);
            case VARCHAR:
            case CHAR:
                return value == null? null: this.convert(value.toString());
            case BIGINT:
                return this.convert((Long) value);
            case DOUBLE:
                return this.convert((Double) value);
            case FLOAT:
                return this.convert((Float) value);
            case DATE:
                return this.convert((LocalDate) value);
            case TIME:
                return this.convert((LocalTime) value);
            case TIMESTAMP:
                return this.convert((LocalDateTime) value);
            case BOOLEAN:
                return this.convert((Boolean) value);
            case UUID:
                return value == null? null: this.convert(UUID.fromString(value.toString()));
            case BLOB:
            case ANY:
                return value == null? null: this.convert(value);
            default:
                throw new RuntimeException(String.format("Type %s doesn't support!", type));
        }
    }
}
