package com.sdu.stream.communicate.thread.disruptor.process.batch.handler;

import com.google.common.collect.Maps;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.TimeoutHandler;
import com.sdu.stream.communicate.thread.disruptor.share.SortEvent;
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
public class FeatureLoader implements EventHandler<SortEvent>, LifecycleAware, TimeoutHandler {

    private String _name;

    private String []_featureNames;

    private static final Random _random = new Random();

    public FeatureLoader(String _name, String[] _featureNames) {
        this._name = _name;
        this._featureNames = _featureNames;
    }

    @Override
    public void onEvent(SortEvent event, long sequence, boolean endOfBatch) throws Exception {
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
        log.info("{} feature loader execute by thread {} ", this._name, Thread.currentThread().getName());
    }

    @Override
    public void onShutdown() {
        log.info("thread pool executor shut down !");
    }

    @Override
    public void onTimeout(long sequence) throws Exception {
        log.info("{} feature loader handler wait for {} sequence timeout !", this._name, sequence + 1);
    }
}
