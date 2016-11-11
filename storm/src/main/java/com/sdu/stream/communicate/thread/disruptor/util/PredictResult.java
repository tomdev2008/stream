package com.sdu.stream.communicate.thread.disruptor.util;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

/**
 * Predict Result
 *
 * @author hanhan.zhang
 * */
@Setter
@Getter
@AllArgsConstructor
public class PredictResult {

    private long cost;

    private long sequence;

    private Map<Integer, Map<String, Double>> featureMap;

    private Map<Integer, Double> scoreMap;
}
