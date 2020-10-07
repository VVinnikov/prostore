package ru.ibs.dtm.common.converter;

import ru.ibs.dtm.common.model.ddl.ColumnType;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

public interface SqlTypeConverter {

    Short convert(Short value);

    Integer convert(Integer value);

    Long convert(Long value);

    Boolean convert(Boolean value);

    BigDecimal convert(BigDecimal value);

    Double convert(Double value);

    Float convert(Float value);

    byte[] convert(byte[] value);

    String convert(String value);

    Timestamp convert(LocalDateTime value);

    Date convert(LocalDate value);

    Time convert(LocalTime value);

    Object convert(Object value);

    Object convert(ColumnType type, Object value);
}
