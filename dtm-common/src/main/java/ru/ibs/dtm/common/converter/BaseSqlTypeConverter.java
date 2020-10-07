package ru.ibs.dtm.common.converter;

import ru.ibs.dtm.common.model.ddl.ColumnType;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;

public class BaseSqlTypeConverter implements SqlTypeConverter {

    @Override
    public Long convert(Long value) {
        return value;
    }

    @Override
    public Boolean convert(Boolean value) {
        return value;
    }

    @Override
    public BigDecimal convert(BigDecimal value) {
        return value;
    }

    @Override
    public Double convert(Double value) {
        return value;
    }

    @Override
    public Float convert(Float value) {
        return value;
    }

    @Override
    public byte[] convert(byte[] value) {
        return value;
    }

    @Override
    public Short convert(Short value) {
        return value;
    }

    @Override
    public Integer convert(Integer value) {
        return value;
    }

    @Override
    public String convert(String value) {
        return value;
    }

    @Override
    public Timestamp convert(LocalDateTime value) {
        return value == null? null: Timestamp.valueOf(value);
    }

    @Override
    public Date convert(LocalDate value) {
        return value == null? null: Date.valueOf(value);
    }

    @Override
    public Time convert(LocalTime value) {
        return value == null? null: Time.valueOf(value);
    }

    @Override
    public Object convert(Object value) {
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
                return this.convert(value);
            default:
                throw new RuntimeException(String.format("Type %s doesn't support!", type));
        }
    }
}
