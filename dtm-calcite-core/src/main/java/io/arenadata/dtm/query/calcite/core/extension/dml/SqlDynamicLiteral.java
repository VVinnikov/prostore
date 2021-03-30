package io.arenadata.dtm.query.calcite.core.extension.dml;

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

public class SqlDynamicLiteral extends SqlLiteral {
    public static final String DYNAMIC_PREFIX = "$";
    /**
     * Creates a <code>SqlLiteral</code>.
     *
     * @param index
     * @param typeName
     * @param pos
     */
    public SqlDynamicLiteral(Object index, SqlTypeName typeName, SqlParserPos pos) {
        super(DYNAMIC_PREFIX + index, typeName, pos);
    }
}
