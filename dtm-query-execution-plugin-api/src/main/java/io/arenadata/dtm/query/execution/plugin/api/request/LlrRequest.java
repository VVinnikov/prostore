/*
 * Copyright © 2021 ProStore
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.arenadata.dtm.query.execution.plugin.api.request;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.reader.QueryParameters;
import io.arenadata.dtm.common.reader.QueryTemplateResult;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.api.dto.PluginRequest;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.UUID;

@Getter
@Setter
public class LlrRequest extends PluginRequest {

    private final QueryTemplateResult sourceQueryTemplateResult;
    private final List<DeltaInformation> deltaInformations;
    private final List<SqlTypeName> parameterTypes;
    private final List<ColumnMetadata> metadata;
    private final QueryParameters parameters;
    private final List<Datamart> schema;
    private final SqlNode originalQuery;
    private final SqlNode withoutViewsQuery;
    private final RelRoot relRoot;
    private final boolean estimate;

    @Builder(toBuilder = true)
    public LlrRequest(UUID requestId,
                      QueryTemplateResult sourceQueryTemplateResult,
                      List<DeltaInformation> deltaInformations,
                      List<SqlTypeName> parameterTypes,
                      List<ColumnMetadata> metadata,
                      QueryParameters parameters,
                      String datamartMnemonic,
                      List<Datamart> schema,
                      SqlNode originalQuery,
                      SqlNode withoutViewsQuery,
                      String envName,
                      RelRoot relRoot,
                      boolean estimate) {
        super(requestId, envName, datamartMnemonic);
        this.sourceQueryTemplateResult = sourceQueryTemplateResult;
        this.deltaInformations = deltaInformations;
        this.parameterTypes = parameterTypes;
        this.originalQuery = originalQuery;
        this.withoutViewsQuery = withoutViewsQuery;
        this.parameters = parameters;
        this.metadata = metadata;
        this.relRoot = relRoot;
        this.schema = schema;
        this.estimate = estimate;
    }
}
