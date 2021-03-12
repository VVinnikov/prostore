package io.arenadata.dtm.query.execution.plugin.adqm.service.impl;

import io.arenadata.dtm.query.execution.plugin.adqm.service.TemplateParameterConverter;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service("adqmTemplateParameterConverter")
public class AdqmTemplateParameterConverter implements TemplateParameterConverter {
    @Override
    public List<SqlNode> convert(List<SqlNode> params) {
        return params.stream()
                .map(this::convertParam)
                .collect(Collectors.toList());
    }

    protected SqlNode convertParam(SqlNode param) {
        if (param instanceof SqlLiteral && SqlTypeName.BOOLEAN.equals(((SqlLiteral) param).getTypeName())) {
            Boolean aBoolean = ((SqlLiteral) param).getValueAs(Boolean.class);
            if (aBoolean == null) {
                return SqlLiteral.createNull(param.getParserPosition());
            } else {
                return SqlLiteral.createExactNumeric(aBoolean ? "1" : "0", param.getParserPosition());
            }
        } else {
            return param;
        }
    }
}
