package ru.ibs.dtm.query.execution.plugin.adqm.converter;

import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.converter.BaseSqlTypeConverter;
import ru.ibs.dtm.common.model.ddl.ColumnType;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

@Component("adqmTypeToSqlTypeConverter")
public class AdqmTypeToSqlTypeConverter extends BaseSqlTypeConverter {

    private static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS";

    @Override
    public Object convert(ColumnType type, Object value) {
        switch (type) {
            case INT:
                return this.convert((Integer) value);
            case VARCHAR:
            case CHAR:
                return value == null? null: this.convert(value.toString());
            case BIGINT:
            case TIME:
                return this.convert((Long) value);
            case DOUBLE:
                return this.convert((Double) value);
            case FLOAT:
                return this.convert((Float) value);
            case DATE:
                return value == null? null: this.convert(toLocalDate((Long) value));
            case TIMESTAMP:
                return value == null? null: this.convert(toLocalDateTime(value.toString()));
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

    private LocalDateTime toLocalDateTime(String value) {
        return LocalDateTime.parse(value, DateTimeFormatter.ofPattern(DATE_TIME_FORMAT));
    }

    private LocalDate toLocalDate(Long value) {
        return LocalDate.ofEpochDay(value);
    }
}
