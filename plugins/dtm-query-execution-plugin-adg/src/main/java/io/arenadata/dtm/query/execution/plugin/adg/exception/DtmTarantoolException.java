package io.arenadata.dtm.query.execution.plugin.adg.exception;

import org.tarantool.TarantoolException;

public class DtmTarantoolException extends TarantoolException {

  public DtmTarantoolException(long code, String message, Throwable cause) {
    super(code, message, cause);
  }

  public DtmTarantoolException(long code, String message) {
    super(code, message);
  }

  public DtmTarantoolException(String message) {
    super(0, message);
  }
}
