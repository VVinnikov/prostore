package io.arenadata.dtm.query.calcite.core.dto;

import lombok.Data;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

@Data
public class EnrichmentTemplateRequest {
    private final SqlNode templateNode;
    private final List<SqlNode> params;
}
