package ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.response;

import lombok.Data;

import java.util.List;

/**
 * Результат операции
 *
 * @data данные
 * @errors список ошибок
 */
@Data
public class ResOperation {
  ResData data;
  List<ResError> errors;
}

