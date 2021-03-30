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
package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.enrichment.context;

import com.google.common.collect.Lists;
import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.exception.DtmException;
import lombok.Builder;
import lombok.Data;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.tools.RelBuilder;

import java.util.List;

@Data
@Builder
public class BuilderContext {
    private List<DeltaInformation> deltaInformations;
    private List<TableScan> tableScans;
    private RelNode lastChildNode;
    private List<RelBuilder> builders;

    public RelBuilder getBuilder() {
        RelBuilder relBuilder = builders.stream()
            .reduce((b1, b2) -> b2.push(b1.build()))
            .orElseThrow(() -> new DtmException("Can't get result relation builder"));
        builders = Lists.newArrayList(relBuilder);
        return relBuilder;
    }
}
