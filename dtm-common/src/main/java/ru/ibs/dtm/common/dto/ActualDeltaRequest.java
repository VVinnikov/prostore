package ru.ibs.dtm.common.dto;

/**
 * DTO поиска актуальной дельты витрины на дату
 */
public class ActualDeltaRequest {
  /**
   * Витрина
   */
  private String datamart;
  /**
   * Datatime в формате 2019-12-23 15:15:14
   */
  private String dateTime;

  public ActualDeltaRequest(String datamart, String dateTime) {
    this.datamart = datamart;
    this.dateTime = dateTime;
  }

  public String getDatamart() {
    return datamart;
  }

  public void setDatamart(String datamart) {
    this.datamart = datamart;
  }

  public String getDateTime() {
    return dateTime;
  }

  public void setDateTime(String dateTime) {
    this.dateTime = dateTime;
  }
}
