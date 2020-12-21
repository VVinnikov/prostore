package io.arenadata.dtm.query.execution.plugin.adg.service;

import java.util.List;

/**
 * Result translator for Tarantool
 */
public interface AdgResultTranslator {
    List<?> translate(List<?> list);
}
