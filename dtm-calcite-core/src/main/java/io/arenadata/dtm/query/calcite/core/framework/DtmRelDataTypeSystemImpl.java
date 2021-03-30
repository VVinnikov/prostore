package io.arenadata.dtm.query.calcite.core.framework;

import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.type.SqlTypeName;

public class DtmRelDataTypeSystemImpl extends RelDataTypeSystemImpl {

    public DtmRelDataTypeSystemImpl() {
        super();
    }

    @Override
    public int getDefaultPrecision(SqlTypeName typeName) {
        switch(typeName) {
            case DECIMAL:
                return this.getMaxNumericPrecision();
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                return 2;
            case CHAR:
            case BINARY:
                return 1;
            case VARCHAR:
            case VARBINARY:
                return -1;
            case BOOLEAN:
                return 1;
            case TINYINT:
                return 3;
            case SMALLINT:
                return 5;
            case INTEGER:
                return 10;
            case BIGINT:
                return 19;
            case REAL:
                return 7;
            case FLOAT:
            case DOUBLE:
                return 15;
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return 6;
            case DATE:
                return 0;
            default:
                return -1;
        }
    }

    @Override
    public int getMaxPrecision(SqlTypeName typeName) {
        switch(typeName) {
            case DECIMAL:
                return this.getMaxNumericPrecision();
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                return 10;
            case CHAR:
            case VARCHAR:
                return 65536;
            case BINARY:
            case VARBINARY:
                return 65536;
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case REAL:
            case FLOAT:
            case DOUBLE:
            case DATE:
            default:
                return this.getDefaultPrecision(typeName);
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return 6;
        }
    }

    @Override
    public boolean shouldConvertRaggedUnionTypesToVarying() {
        return true;
    }
}
