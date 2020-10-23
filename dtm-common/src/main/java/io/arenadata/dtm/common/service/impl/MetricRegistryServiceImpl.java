package io.arenadata.dtm.common.service.impl;

import io.arenadata.dtm.common.model.DurationStatistic;
import io.arenadata.dtm.common.service.MetricRegistryService;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class MetricRegistryServiceImpl implements MetricRegistryService {

  private AtomicBoolean isModeOn = new AtomicBoolean(true);

  private ConcurrentHashMap<String, DurationStatistic> map = new ConcurrentHashMap<>();

  public void turnOff(){
    isModeOn.set(false);
  }
  public void turnOn(){
    isModeOn.set(true);
  }

  @Override
  public void append(String operation, Long duration) {
    if (isModeOn.get()) {
      DurationStatistic stats = map.computeIfAbsent(operation, DurationStatistic::new);
      stats.add(duration);
    }
  }

  @Override
  public <T> T measureTimeMillis(String operation, Supplier<T> block) {
    long start=0L;
    if(isModeOn.get()) {
      start = System.currentTimeMillis();
    }
    T result = block.get();
    if(isModeOn.get()) {
      append(operation, System.currentTimeMillis() - start);
    }
    return result;
  }

  @Override
  public String printSummaryStatistics() {
    if(isModeOn.get()) {
      StringBuilder sb = new StringBuilder();
      sb.append("Статистика по операциям:\nОперация; Количество; Сумма; Макс; Мин; Среднее\n")
        .append(map.values().stream().map(DurationStatistic::toString).collect(Collectors.joining("\n")));
      return sb.toString();
    } else
      return "Измерение метрик отключено";
  }

}
