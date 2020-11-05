package io.arenadata.dtm.query.execution.core.service.cache.key;

import java.util.ArrayList;
import java.util.Arrays;

public class EntityKey extends ArrayList<String> {
    public EntityKey(String datamartName, String entityName) {
        super(Arrays.asList(datamartName, entityName));
    }
}
