package com.sdu.stream.communicate.thread.disruptor.share;

import com.lmax.disruptor.RingBuffer;
import lombok.AllArgsConstructor;

import java.util.List;

/**
 * Predict Event Producer
 *
 * @author hanhan.zhang
 * */
@AllArgsConstructor
public class EventProducer {

    private RingBuffer<PredictEvent> _ringBuffer;

    public long sort(List<Integer> predictItems) {
        long nextSequence = this._ringBuffer.next();
        try {
            PredictEvent predictEvent = this._ringBuffer.get(nextSequence);
            predictEvent.setSequence(nextSequence);
            predictEvent.setStart(System.currentTimeMillis());
            predictEvent.setPredictItems(predictItems);
        } finally {
            // publish event
            this._ringBuffer.publish(nextSequence);
        }
        return nextSequence;
    }
}
