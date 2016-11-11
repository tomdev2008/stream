package com.sdu.stream.communicate.thread.disruptor.process.custom;

import com.lmax.disruptor.*;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Custom Event Process
 *
 * @author hanhan.zhang
 * */
public class CustomEventProcess<T> implements EventProcessor {
    private final AtomicBoolean running = new AtomicBoolean(false);

    private final DataProvider<T> dataProvider;

    private final SequenceBarrier sequenceBarrier;

    private final EventHandler<? super T> eventHandler;

    private final Sequence sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);

    public CustomEventProcess(DataProvider<T> dataProvider, SequenceBarrier sequenceBarrier, EventHandler<? super T> eventHandler) {
        this.dataProvider = dataProvider;
        this.sequenceBarrier = sequenceBarrier;
        this.eventHandler = eventHandler;
    }

    @Override
    public Sequence getSequence() {
        return sequence;
    }

    @Override
    public void halt() {
        sequenceBarrier.alert();
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public void run() {

    }
}
