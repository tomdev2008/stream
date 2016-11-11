package com.sdu.stream.communicate.thread.disruptor.share;

import com.lmax.disruptor.EventFactory;

/**
 * PredictEvent Factory
 *
 * @author hanhan.zhang
 * */
public class PredictEventFactory implements EventFactory<PredictEvent> {

    @Override
    public PredictEvent newInstance() {
        return new PredictEvent();
    }

}
