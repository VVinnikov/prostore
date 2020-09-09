package ru.ibs.dtm.query.calcite.core.framework;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

public class DtmCalciteSqlValidator extends SqlValidatorImpl {

    DtmCalciteSqlValidator(SqlOperatorTable opTab, CalciteCatalogReader catalogReader, JavaTypeFactory typeFactory, SqlConformance conformance) {
        super(opTab, catalogReader, typeFactory, conformance);
    }

    protected RelDataType getLogicalSourceRowType(RelDataType sourceRowType, SqlInsert insert) {
        RelDataType superType = super.getLogicalSourceRowType(sourceRowType, insert);
        return ((JavaTypeFactory) this.typeFactory).toSql(superType);
    }

    protected RelDataType getLogicalTargetRowType(RelDataType targetRowType, SqlInsert insert) {
        RelDataType superType = super.getLogicalTargetRowType(targetRowType, insert);
        return ((JavaTypeFactory) this.typeFactory).toSql(superType);
    }
}
