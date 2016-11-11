package com.sdu.stream.communicate.thread.disruptor.process.batch.handler;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.TimeoutHandler;
import com.sdu.stream.communicate.thread.disruptor.share.PredictEvent;
import com.sdu.stream.communicate.thread.disruptor.util.PredictResult;
import com.sdu.stream.communicate.thread.disruptor.util.PredictTracker;
import lombok.extern.slf4j.Slf4j;

/**
 * Sort Response
 *
 * @author hanhan.zhang
 * */
@Slf4j
public class ResponseHandler implements EventHandler<PredictEvent>, LifecycleAware, TimeoutHandler {

    @Override
    public void onEvent(PredictEvent event, long sequence, boolean endOfBatch) throws Exception {
        long cost = System.currentTimeMillis() - event.getStart();
        PredictResult predictResult = new PredictResult(cost, sequence, event.getFeatureMap(), event.getPredictItemScoreMap());
        PredictTracker.addPredictResult(sequence, predictResult);
    }

    @Override
    public void onStart() {

    }

    @Override
    public void onShutdown() {

    }

    @Override
    public void onTimeout(long sequence) throws Exception {

    }
}
