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
package io.arenadata.dtm.query.execution.plugin.adb;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.plugin.api.AbstractDtmDataSourcePlugin;
import io.arenadata.dtm.query.execution.plugin.api.service.DdlService;
import io.arenadata.dtm.query.execution.plugin.api.service.LlrService;
import io.arenadata.dtm.query.execution.plugin.api.service.RollbackService;
import io.arenadata.dtm.query.execution.plugin.api.service.StatusService;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckDataService;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckTableService;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import io.arenadata.dtm.query.execution.plugin.api.service.mppr.MpprService;
import io.arenadata.dtm.query.execution.plugin.api.service.mppw.MppwService;

import java.util.Collections;
import java.util.Set;

public class AdbDtmDataSourcePlugin extends AbstractDtmDataSourcePlugin {

    public static final String ADB_QUERY_TEMPLATE_CACHE = "adbQueryTemplateCache";
    public static final String ADB_DATAMART_CACHE = "adb_datamart";

    public AdbDtmDataSourcePlugin(
            DdlService<Void> ddlService,
            LlrService<QueryResult> adbLlrService,
            MpprService adbMpprService,
            MppwService adbMppwService,
            StatusService statusService,
            RollbackService<Void> rollbackService,
            CheckTableService checkTableService,
            CheckDataService checkDataService,
            TruncateHistoryService truncateService) {
        super(ddlService,
                adbLlrService,
                adbMpprService,
                adbMppwService,
                statusService,
                rollbackService,
                checkTableService,
                checkDataService,
                truncateService);
    }

    @Override
    public SourceType getSourceType() {
        return SourceType.ADB;
    }

    @Override
    public Set<String> getActiveCaches() {
        return Collections.singleton(ADB_DATAMART_CACHE);
    }
}
