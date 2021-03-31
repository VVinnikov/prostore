package io.arenadata.dtm.async;

@FunctionalInterface
public interface DurationListener {
    void onDuration(Long duration);
}
