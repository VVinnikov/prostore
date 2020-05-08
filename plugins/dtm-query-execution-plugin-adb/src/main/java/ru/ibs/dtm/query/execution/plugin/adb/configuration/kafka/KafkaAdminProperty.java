package ru.ibs.dtm.query.execution.plugin.adb.configuration.kafka;

public class KafkaAdminProperty {
  /** Топик запроса. */
  String adbUploadRq = "";
  /** Топик ответа от запроса к ADB. */
  String adbUploadRs = "";
  /** Топик ошибок от запроса к ADB. */
  String adbUploadErr = "";

  public String getAdbUploadRq() {
    return adbUploadRq;
  }

  public void setAdbUploadRq(String adbUploadRq) {
    this.adbUploadRq = adbUploadRq;
  }

  public String getAdbUploadRs() {
    return adbUploadRs;
  }

  public void setAdbUploadRs(String adbUploadRs) {
    this.adbUploadRs = adbUploadRs;
  }

  public String getAdbUploadErr() {
    return adbUploadErr;
  }

  public void setAdbUploadErr(String adbUploadErr) {
    this.adbUploadErr = adbUploadErr;
  }
}
