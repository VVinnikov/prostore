package ru.ibs.dtm.query.execution.plugin.adg.converter;

import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.converter.BaseSqlTypeConverter;
import ru.ibs.dtm.common.model.ddl.ColumnType;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.UUID;

@Component("adgTypeToSqlTypeConverter")
public class AdgTypeToSqlTypeConverter extends BaseSqlTypeConverter {

    @Override
    public Object convert(ColumnType type, Object value) {
        switch (type) {
            case INT:
                return this.convert((Integer) value);
            case VARCHAR:
            case CHAR:
                return value == null ? null : this.convert(value.toString());
            case BIGINT:
            case TIME:
                return value == null ? null : value.getClass().equals(Integer.class) ?
                        this.convert(((Integer) value).longValue()) : this.convert((Long) value);
            case DOUBLE:
                return this.convert((Double) value);
            case FLOAT:
                return this.convert((Float) value);
            case DATE:
                return value == null ? null : value.getClass().equals(Integer.class) ?
                        this.convert(toLocalDate(((Integer) value).longValue())) : this.convert(toLocalDate((Long) value));
            case TIMESTAMP:
                return value == null ? null : this.convert(toLocalDateTime((Long) value));
            case BOOLEAN:
                return this.convert((Boolean) value);
            case UUID:
                return value == null ? null : this.convert(UUID.fromString(value.toString()));
            case BLOB:
            case ANY:
                return value == null ? null : this.convert(value);
            default:
                throw new RuntimeException(String.format("Type %s doesn't support!", type));
        }
    }

    private LocalDateTime toLocalDateTime(Long value) {
        //TODO implement getting ZoneId from configuration
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(value), ZoneId.systemDefault());
    }

    private LocalDate toLocalDate(Long value) {
        //TODO implement getting ZoneId from configuration
        return LocalDate.ofEpochDay(value);
    }
}
