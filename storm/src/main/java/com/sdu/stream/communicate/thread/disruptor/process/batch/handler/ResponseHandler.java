package com.sdu.stream.communicate.thread.disruptor.process.batch.handler;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.TimeoutHandler;
import com.sdu.stream.communicate.thread.disruptor.share.SortEvent;
import lombok.extern.slf4j.Slf4j;

/**
 * Sort Response
 *
 * @author hanhan.zhang
 * */
@Slf4j
public class ResponseHandler implements EventHandler<SortEvent>, LifecycleAware, TimeoutHandler {

    @Override
    public void onEvent(SortEvent event, long sequence, boolean endOfBatch) throws Exception {
        long start = event.getStart();
        log.info("sort cost : {} ms ", System.currentTimeMillis() - start);
    }

    @Override
    public void onStart() {
        log.info("response handler execute by thread {} ", Thread.currentThread().getName());
    }

    @Override
    public void onShutdown() {

    }

    @Override
    public void onTimeout(long sequence) throws Exception {
        log.info("response handler wait for {} sequence timeout !", sequence + 1);
    }
}
