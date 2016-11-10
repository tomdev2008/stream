package com.sdu.stream.communicate.thread.disruptor.process.batch.handler;

import com.google.common.collect.Maps;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.TimeoutHandler;
import com.sdu.stream.communicate.thread.disruptor.share.SortEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * Score Handler
 *
 * @author hanhan.zhang
 * */
@Slf4j
public class ScoreHandler implements EventHandler<SortEvent>, LifecycleAware, TimeoutHandler {

    @Override
    public void onEvent(SortEvent event, long sequence, boolean endOfBatch) throws Exception {
        Map<Integer, Double> sortMap = Maps.newHashMap();
        event.getFeatureMap().forEach((item, featureMap) -> {
            double score = 0.0;
            for (Map.Entry<String, Double> entry : featureMap.entrySet()) {
                score += entry.getValue();
            }
            sortMap.put(item, score);
        });
        event.setPredictItemScoreMap(sortMap);
    }

    @Override
    public void onStart() {
        log.info("score handler execute by thread {} ", Thread.currentThread().getName());
    }

    @Override
    public void onShutdown() {

    }

    @Override
    public void onTimeout(long sequence) throws Exception {
        log.info("score handler wait for {} sequence timeout !", sequence + 1);
    }
}
