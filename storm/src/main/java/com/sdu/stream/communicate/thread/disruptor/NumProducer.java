package com.sdu.stream.communicate.thread.disruptor;

import com.lmax.disruptor.WorkHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NumEvent Producer
 *
 * @author hanhan.zhang
 * */
public class NumProducer implements WorkHandler<NumEvent> {

    private static final Logger LOGGER = LoggerFactory.getLogger(NumProducer.class);

    private int seq = 0;

    @Override
    public void onEvent(NumEvent event) throws Exception {
        int curNum = ++seq;
        LOGGER.info("producer generate num : {} .", curNum);
        event.setValue(curNum);
    }
}
