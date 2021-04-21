package io.arenadata.dtm.query.execution.core.dml.dto;

import io.arenadata.dtm.common.cache.SourceQueryTemplateValue;
import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.reader.QuerySourceRequest;
import lombok.Builder;
import lombok.Data;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

@Data
public class LlrRequestContext {
    private List<DeltaInformation> deltaInformations;
    private DmlRequestContext dmlRequestContext;
    private QuerySourceRequest sourceRequest;
    private SourceQueryTemplateValue queryTemplateValue;
    private RelRoot relNode;
    private SqlNode originalQuery;

    @Builder
    public LlrRequestContext(List<DeltaInformation> deltaInformations,
                             DmlRequestContext dmlRequestContext,
                             QuerySourceRequest sourceRequest,
                             SqlNode originalQuery) {
        this.deltaInformations = deltaInformations;
        this.dmlRequestContext = dmlRequestContext;
        this.sourceRequest = sourceRequest;
        this.originalQuery = originalQuery;
    }
}