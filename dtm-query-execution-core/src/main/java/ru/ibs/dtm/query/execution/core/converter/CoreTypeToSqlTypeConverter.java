package ru.ibs.dtm.query.execution.core.converter;

import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.converter.BaseSqlTypeConverter;
import ru.ibs.dtm.common.model.ddl.ColumnType;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

@Component("coreTypeToSqlTypeConverter")
public class CoreTypeToSqlTypeConverter extends BaseSqlTypeConverter {

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
                return value == null? null: this.convert(toLocalDateTime(value.toString()));
            case BOOLEAN:
                return this.convert((Boolean) value);
            case UUID:
                return value == null? null: this.convert(UUID.fromString(value.toString()));
            case BLOB:
            case ANY:
                return this.convert(value);
            default:
                throw new RuntimeException(String.format("Type %s doesn't support!", type));
        }
    }

    private LocalDateTime toLocalDateTime(String value) {
        return LocalDateTime.parse(value, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    }
}
