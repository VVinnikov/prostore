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
package io.arenadata.dtm.query.execution.core.service.dml.impl;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.service.dml.SourceTypePriorityService;
import org.springframework.stereotype.Service;

import java.util.Set;

@Service
public class SimpleSourceTypePriorityService implements SourceTypePriorityService {
    @Override
    public SourceType prioritize(Set<SourceType> sourceTypes) {
        if (sourceTypes.size() == 1) {
            return sourceTypes.iterator().next();
        } else {
            final SourceType defaultSourceType;
            if (sourceTypes.contains(SourceType.ADB)) {
                defaultSourceType = SourceType.ADB;
            } else if (sourceTypes.contains(SourceType.ADG)) {
                defaultSourceType = SourceType.ADG;
            } else if (sourceTypes.contains(SourceType.ADQM)) {
                defaultSourceType = SourceType.ADQM;
            } else {
                throw new DtmException("Can't get priority source type from active plugins");
            }
            return defaultSourceType;
        }
    }
}
