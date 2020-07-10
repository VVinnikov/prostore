package ru.ibs.dtm.query.calcite.core.extension.eddl;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class SqlCreateUploadExternalTable extends SqlCreate {

    private final SqlIdentifier name;
    private final SqlNodeList columnList;
    private final LocationOperator locationOperator;
    private final FormatOperator formatOperator;
    private final MassageLimitOperator massageLimitOperator;

    private static final SqlOperator OPERATOR_TABLE =
            new SqlSpecialOperator("CREATE UPLOAD EXTERNAL TABLE", SqlKind.OTHER_DDL);

    public SqlCreateUploadExternalTable(SqlParserPos pos, boolean ifNotExists, SqlIdentifier name,
                                        SqlNodeList columnList, SqlNode location, SqlNode format, SqlNode chunkSize) {
        super(OPERATOR_TABLE, pos, false, ifNotExists);
        this.name = Objects.requireNonNull(name);
        this.columnList = columnList;
        this.locationOperator = new LocationOperator(pos, (SqlCharStringLiteral) location);
        this.formatOperator = new FormatOperator(pos, (SqlCharStringLiteral) format);
        this.massageLimitOperator = new MassageLimitOperator(pos, (SqlNumericLiteral) chunkSize);
    }

    public List<SqlNode> getOperandList() {
        return ImmutableList.of(this.name, this.columnList, this.locationOperator, this.formatOperator, this.massageLimitOperator);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(this.getOperator().getName());
        this.name.unparse(writer, leftPrec, rightPrec);
        if (this.columnList != null) {
            SqlWriter.Frame frame = writer.startList("(", ")");
            Iterator columnIterator = this.columnList.iterator();
            while (columnIterator.hasNext()) {
                SqlNode c = (SqlNode) columnIterator.next();
                writer.sep(",");
                c.unparse(writer, 0, 0);
            }
            writer.endList(frame);
        }

        this.locationOperator.unparse(writer, leftPrec, rightPrec);
        this.formatOperator.unparse(writer, leftPrec, rightPrec);
        this.massageLimitOperator.unparse(writer, leftPrec, rightPrec);
    }

    public SqlIdentifier getName() {
        return name;
    }

    public SqlNodeList getColumnList() {
        return columnList;
    }

    public LocationOperator getLocationOperator() {
        return locationOperator;
    }

    public FormatOperator getFormatOperator() {
        return formatOperator;
    }

    public MassageLimitOperator getMassageLimitOperator() {
        return massageLimitOperator;
    }
}
