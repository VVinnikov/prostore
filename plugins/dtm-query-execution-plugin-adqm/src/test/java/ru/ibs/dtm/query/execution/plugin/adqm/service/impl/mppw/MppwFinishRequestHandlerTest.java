package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.mppw;

import org.junit.jupiter.api.Test;
import ru.ibs.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import ru.ibs.dtm.query.execution.plugin.adqm.service.mock.MockDatabaseExecutor;

import java.util.Collections;

class MppwFinishRequestHandlerTest {
    @Test
    public void testFinishRequestCallOrder() {
        DatabaseExecutor executor = new MockDatabaseExecutor(Collections.emptyList());

//        MppwRequestHandler handler = new MppwFinishRequestHandler();
    }
}