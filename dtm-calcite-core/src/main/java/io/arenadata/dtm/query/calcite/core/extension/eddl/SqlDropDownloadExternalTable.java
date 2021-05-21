package io.arenadata.dtm.query.calcite.core.extension.eddl;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlDropDownloadExternalTable extends SqlDropExternalTable {
    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("DROP DOWNLOAD EXTERNAL TABLE", SqlKind.OTHER_DDL);

    public SqlDropDownloadExternalTable(SqlParserPos pos, boolean ifExists, SqlIdentifier name) {
        super(OPERATOR, pos, ifExists, name);
    }
}
