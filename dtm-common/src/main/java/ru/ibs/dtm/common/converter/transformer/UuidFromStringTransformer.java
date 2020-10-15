package ru.ibs.dtm.common.converter.transformer;

import ru.ibs.dtm.common.model.ddl.ColumnType;

import java.util.UUID;

public class UuidFromStringTransformer implements ColumnTransformer<UUID, String> {

    @Override
    public UUID transform(String value) {
        return value == null ? null : UUID.fromString(value);
    }

    @Override
    public Class<?> getTransformClass() {
        return String.class;
    }

    @Override
    public ColumnType getType() {
        return ColumnType.UUID;
    }
}
