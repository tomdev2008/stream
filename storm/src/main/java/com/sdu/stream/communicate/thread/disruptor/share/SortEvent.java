package com.sdu.stream.communicate.thread.disruptor.share;

import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

/**
 * Sort Result(share data by thread)
 *
 * @author hanhan.zhang
 * */
@Setter
@Getter
public class SortEvent {

    private long start;

    // sort list
    private List<Integer> predictItems;

    // feature
    private Map<Integer, Map<String, Double>> featureMap;

    // sort result
    private Map<Integer, Double> predictItemScoreMap;
}
