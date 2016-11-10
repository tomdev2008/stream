package com.sdu.stream.communicate.thread.disruptor.process.custom;

import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.Sequence;

/**
 * Custom Event Process
 *
 * @author hanhan.zhang
 * */
public class CustomEventProcess implements EventProcessor {

    @Override
    public Sequence getSequence() {
        return null;
    }

    @Override
    public void halt() {

    }

    @Override
    public boolean isRunning() {
        return false;
    }

    @Override
    public void run() {

    }
}
