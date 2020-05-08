package ru.ibs.dtm.query.execution.plugin.adg.service;

import java.util.List;

/**
 * Обработчик результата выполнения
 */
public interface TtResultTranslator {
  List<?> translate(List<?> list);
}
