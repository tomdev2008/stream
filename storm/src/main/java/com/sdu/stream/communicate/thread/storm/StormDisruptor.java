package com.sdu.stream.communicate.thread.storm;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.dsl.ProducerType;
import com.sdu.stream.communicate.thread.storm.listener.ConsumerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 *
 * */
public class StormDisruptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(StormDisruptor.class);

    protected static class StormEventHandler implements EventHandler<Object> {

        @Override
        public void onEvent(Object event, long sequence, boolean endOfBatch) throws Exception {
            LOGGER.info("sequence = {}, uuid = {}", sequence, event);
        }

    }

    protected static class StormEventConsumer implements ConsumerListener {

        @Override
        public EventHandler<Object> getConsumerHandler() {
            return new StormEventHandler();
        }

    }

    public static void main(String[] args) {
        StormDisruptorQueue queue = new StormDisruptorQueue("storm", ProducerType.SINGLE, 1024, 20, 10, 5L, new StormEventConsumer());

        while (true) {
            String uuid = UUID.randomUUID().toString();
            queue.publish(uuid);
        }
    }
}
