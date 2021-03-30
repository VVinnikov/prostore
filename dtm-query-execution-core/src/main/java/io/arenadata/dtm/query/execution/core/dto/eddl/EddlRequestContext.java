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
package io.arenadata.dtm.query.execution.core.dto.eddl;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.execution.core.dto.CoreRequestContext;
import lombok.Builder;
import lombok.ToString;
import org.apache.calcite.sql.SqlNode;

import static io.arenadata.dtm.common.model.SqlProcessingType.EDDL;

@ToString
public class EddlRequestContext extends CoreRequestContext<DatamartRequest, SqlNode> {

    @Builder
    public EddlRequestContext(RequestMetrics metrics,
                              DatamartRequest request,
                              String envName,
                              SqlNode sqlNode) {
        super(metrics, envName, request, sqlNode);
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return EDDL;
    }
}
