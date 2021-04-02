package io.arenadata.dtm.query.execution.plugin.adg.base.service.client;

import java.util.List;

/**
 * Result translator for Tarantool
 */
public interface AdgResultTranslator {
    List<Object> translate(List<Object> list);
}
