package com.sdu.stream.communicate.thread.disruptor.process.batch.handler;

import com.google.common.collect.Maps;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.TimeoutHandler;
import com.sdu.stream.communicate.thread.disruptor.share.PredictEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;

/**
 * Feature Loader
 *
 * Note:
    {@link BatchEventProcessor}'s {@link EventHandler} can implements {@link LifecycleAware} and {@link TimeoutHandler}
 *
 * @author hanhan.zhang
 * */
@Slf4j
public class FeatureLoader implements EventHandler<PredictEvent>, LifecycleAware, TimeoutHandler {

    private String []_featureNames;

    private static final Random _random = new Random();

    public FeatureLoader(String[] _featureNames) {
        this._featureNames = _featureNames;
    }

    @Override
    public void onEvent(PredictEvent event, long sequence, boolean endOfBatch) throws Exception {
        Map<Integer, Map<String, Double>> featureMap = event.getFeatureMap();
        if (featureMap == null) {
            featureMap = Maps.newLinkedHashMapWithExpectedSize(event.getPredictItems().size());
        }
        for (int item : event.getPredictItems()) {
            Map<String, Double> newFeatureMap = getItemFeature(item);
            Map<String, Double> oldFeatureMap = featureMap.get(item);
            if (oldFeatureMap == null) {
                featureMap.put(item, newFeatureMap);
            } else {
                oldFeatureMap.putAll(newFeatureMap);
            }
        }
        event.setFeatureMap(featureMap);
    }

    public  final Map<String, Double> getItemFeature(int item) {
        Map<String, Double> featureMap = Maps.newHashMap();
        Arrays.stream(_featureNames).forEach(featureName ->
            featureMap.put(featureName, _random.nextDouble())
        );
        return featureMap;
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
