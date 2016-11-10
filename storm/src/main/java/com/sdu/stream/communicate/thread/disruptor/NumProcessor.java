package com.sdu.stream.communicate.thread.disruptor;

import com.lmax.disruptor.WorkHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NumEvent Consumer
 *
 * @author hanhan.zhang
 * */
public class NumProcessor implements WorkHandler<NumEvent> {

    private static final Logger LOGGER = LoggerFactory.getLogger(NumProcessor.class);

    @Override
    public void onEvent(NumEvent event) throws Exception {
        int value = event.getValue();
        LOGGER.info("consumer obtain num : {} .", value);
    }
}
