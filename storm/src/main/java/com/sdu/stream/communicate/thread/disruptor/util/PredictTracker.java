package com.sdu.stream.communicate.thread.disruptor.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Predict Result Holder(Thread Safe)
 *
 * @author hanhan.zhang
 * */
public class PredictTracker {

    private static final ConcurrentHashMap<Long, PredictResult> resultMap = new ConcurrentHashMap<>();


    public static PredictResult getPredictResult(long sequence, boolean remove) throws Exception {
        TimeUnit.SECONDS.sleep(1);
        if (remove) {
            return resultMap.remove(sequence);
        }
        return resultMap.get(sequence);
    }

    public static void addPredictResult(long sequence, PredictResult predictResult) {
        resultMap.put(sequence, predictResult);
    }

}
