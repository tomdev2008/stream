package com.sdu.stream.communicate.thread.disruptor.share;

import com.lmax.disruptor.RingBuffer;
import lombok.AllArgsConstructor;

import java.util.List;

/**
 * Sort Request Producer
 *
 * @author hanhan.zhang
 * */
@AllArgsConstructor
public class SortRequestProducer {

    private RingBuffer<SortEvent> _ringBuffer;

    public void sort(List<Integer> predictItems) {
        long nextSequence = this._ringBuffer.next();
        try {
            SortEvent sortRequest = this._ringBuffer.get(nextSequence);
            sortRequest.setStart(System.currentTimeMillis());
            sortRequest.setPredictItems(predictItems);
        } finally {
            // commit
            this._ringBuffer.publish(nextSequence);
        }
    }
}
