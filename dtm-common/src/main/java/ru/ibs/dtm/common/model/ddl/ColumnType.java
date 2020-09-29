package ru.ibs.dtm.common.model.ddl;

import java.sql.Types;

/**
 * Logical model data types
 */
public enum ColumnType {
    VARCHAR(Types.VARCHAR, new String[]{"varchar"}),
    CHAR(Types.CHAR, new String[]{"char"}),
    BIGINT(Types.BIGINT, new String[]{"bigint"}),
    INT(Types.INTEGER, new String[]{"int", "integer"}),
    DOUBLE(Types.DOUBLE, new String[]{"double"}),
    FLOAT(Types.FLOAT, new String[]{"float"}),
    DATE(Types.DATE, new String[]{"date"}),
    TIME(Types.TIME, new String[]{"time"}),
    TIMESTAMP(Types.TIMESTAMP, new String[]{"timestamp"}),
    BOOLEAN(Types.BOOLEAN, new String[]{"boolean"}),
    BLOB(Types.BLOB, new String[]{"blob"}),
    UUID(Types.OTHER, new String[]{"uuid"}),
    ANY(Types.OTHER, new String[]{"any"});

    private final int sqlType;
    private final String[] aliases;

    ColumnType(int sqlType, String[] aliases) {
        this.sqlType = sqlType;
        this.aliases = aliases;
    }

    public static ColumnType fromTypeString(String typeString) {
        String s = typeString.trim();
        ColumnType[] types = values();
        for (ColumnType dataType : types) {
            if (s.equalsIgnoreCase(dataType.name())) {
                return dataType;
            }
            for (int j = 0; j < dataType.aliases.length; ++j) {
                String alias = dataType.aliases[j];
                if (s.equalsIgnoreCase(alias)) {
                    return dataType;
                }
            }
        }
        return ANY;
    }

    public int getSqlType() {
        return sqlType;
    }

    public String[] getAliases() {
        return aliases;
    }
}
