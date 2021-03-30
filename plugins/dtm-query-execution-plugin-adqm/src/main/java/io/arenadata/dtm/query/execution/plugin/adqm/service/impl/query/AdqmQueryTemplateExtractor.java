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
package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.query;

import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.calcite.core.service.impl.AbstractQueryTemplateExtractor;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service("adqmQueryTemplateExtractor")
public class AdqmQueryTemplateExtractor extends AbstractQueryTemplateExtractor {

    @Autowired
    public AdqmQueryTemplateExtractor(@Qualifier("adqmCalciteDefinitionService") DefinitionService<SqlNode> definitionService,
                                      @Qualifier("adqmSqlDialect") SqlDialect sqlDialect) {
        super(definitionService, sqlDialect);
    }
}
