package io.arenadata.dtm.query.execution.core.service.dml;

import io.arenadata.dtm.common.dml.SelectCategory;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

public interface SelectCategoryQualifier {
    SelectCategory qualify(List<Datamart> schema, SqlNode query);
}