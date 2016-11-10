package com.sdu.stream.communicate.thread.disruptor.share;

import com.lmax.disruptor.EventFactory;
import com.sdu.stream.communicate.thread.disruptor.share.SortEvent;

/**
 * SortEvent Factory
 *
 * @author hanhan.zhang
 * */
public class SortEventFactory implements EventFactory<SortEvent> {

    @Override
    public SortEvent newInstance() {
        return new SortEvent();
    }

}
