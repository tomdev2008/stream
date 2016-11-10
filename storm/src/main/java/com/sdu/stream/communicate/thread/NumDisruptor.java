package com.sdu.stream.communicate.thread;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.ProducerType;
import com.sdu.stream.communicate.thread.disruptor.NumEvent;
import com.sdu.stream.communicate.thread.disruptor.NumProcessor;
import com.sdu.stream.communicate.thread.disruptor.NumProducer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 *
 *
 * @author hanhan.zhang
 * */
public class NumDisruptor {

    public static class NumEventFactory implements EventFactory<NumEvent> {
        @Override
        public NumEvent newInstance() {
            return new NumEvent();
        }
    }

    public static class NumEventExceptionHandler implements ExceptionHandler<NumEvent> {

        @Override
        public void handleEventException(Throwable ex, long sequence, NumEvent event) {

        }

        @Override
        public void handleOnStartException(Throwable ex) {

        }

        @Override
        public void handleOnShutdownException(Throwable ex) {

        }
    }

    public static void main(String[] args) throws Exception {
        RingBuffer<NumEvent> ringBuffer = RingBuffer.create(ProducerType.SINGLE, new NumEventFactory(), 1024, new YieldingWaitStrategy());

        // producer
        SequenceBarrier producerSequenceBarrier = ringBuffer.newBarrier();
        NumProducer producer = new NumProducer();
        WorkerPool<NumEvent> producerWorkPool = new WorkerPool<>(ringBuffer, producerSequenceBarrier, new NumEventExceptionHandler(), producer);

        // consumer
        SequenceBarrier consumerSequenceBarrier = ringBuffer.newBarrier(producerWorkPool.getWorkerSequences());
        NumProcessor []consumer = new NumProcessor[]{new NumProcessor(), new NumProcessor()};
        WorkerPool<NumEvent> consumerWorkPool = new WorkerPool<NumEvent>(ringBuffer, consumerSequenceBarrier, new NumEventExceptionHandler(), consumer);

        List<Sequence> gatingSequences = new ArrayList<Sequence>();
        gatingSequences.addAll(Arrays.asList(producerWorkPool.getWorkerSequences()));
        gatingSequences.addAll(Arrays.asList(consumerWorkPool.getWorkerSequences()));

        ringBuffer.addGatingSequences(gatingSequences.toArray(new Sequence[gatingSequences.size()]));

        ThreadPoolExecutor executor = new ThreadPoolExecutor(5, 10, 5, TimeUnit.MINUTES, new LinkedBlockingDeque<>(1024));
        producerWorkPool.start(executor);
        consumerWorkPool.start(executor);

        while (true) {
            TimeUnit.SECONDS.sleep(1);
            ringBuffer.publish(ringBuffer.next());
        }
    }

}
