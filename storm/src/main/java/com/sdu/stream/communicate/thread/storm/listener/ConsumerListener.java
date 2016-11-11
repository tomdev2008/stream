package com.sdu.stream.communicate.thread.storm.listener;

import com.lmax.disruptor.EventHandler;

/**
 * Disruptor Consume Listener
 *
 * @author hanhan.zhang
 * */
public interface ConsumerListener {

    public EventHandler<Object> getConsumerHandler();

}
