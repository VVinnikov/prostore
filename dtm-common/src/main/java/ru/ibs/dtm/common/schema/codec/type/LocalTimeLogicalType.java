package ru.ibs.dtm.common.schema.codec.type;

import org.apache.avro.LogicalType;

import java.time.LocalTime;

public class LocalTimeLogicalType extends LogicalType {

    public static LocalTimeLogicalType INSTANCE = new LocalTimeLogicalType();

    public LocalTimeLogicalType() {
        super(LocalTime.class.getSimpleName());
    }
}
