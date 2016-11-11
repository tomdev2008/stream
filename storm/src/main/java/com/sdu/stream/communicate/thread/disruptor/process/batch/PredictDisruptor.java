package com.sdu.stream.communicate.thread.disruptor.process.batch;

import com.google.common.collect.Lists;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.sdu.stream.communicate.thread.disruptor.process.batch.handler.ScoreHandler;
import com.sdu.stream.communicate.thread.disruptor.process.batch.handler.FeatureLoader;
import com.sdu.stream.communicate.thread.disruptor.process.batch.handler.ResponseHandler;
import com.sdu.stream.communicate.thread.disruptor.share.PredictEvent;
import com.sdu.stream.communicate.thread.disruptor.share.PredictEventFactory;
import com.sdu.stream.communicate.thread.disruptor.share.EventProducer;

import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Sort Disruptor
 *
 * @author hanhan.zhang
 * */
public class PredictDisruptor {

    private static final Random random = new Random();

    public static final List<Integer> generatePredictItem(int size) {
        List<Integer> predictItemList = Lists.newLinkedList();
        for (int i = 0; i < size; i++) {
            predictItemList.add(random.nextInt(100));
        }
        return predictItemList;
    }

    public static void main(String[] args) throws Exception {

        //单生产者模式
        Disruptor<PredictEvent> disruptor = new Disruptor<>(new PredictEventFactory(), 1024, Executors.newCachedThreadPool(), ProducerType.SINGLE, new YieldingWaitStrategy());

        // feature loader
        EventHandler<PredictEvent> ctrFeatureLoader = new FeatureLoader(new String[]{"ctr_7", "ctr_20", "ctr_30"});
        EventHandler<PredictEvent> cvrFeatureLoader = new FeatureLoader(new String[]{"cvr_7", "cvr_20", "cvr_30"});

        // feature score
        EventHandler<PredictEvent> sortHandler = new ScoreHandler();

        // response
        EventHandler<PredictEvent> response = new ResponseHandler();

        // DSL(Use BatchEventProcessor)
        disruptor.handleEventsWith(ctrFeatureLoader, cvrFeatureLoader)
                .then(sortHandler)
                .then(response);


        disruptor.start();


        RingBuffer<PredictEvent> ringBuffer = disruptor.getRingBuffer();

        // producer
        EventProducer producer = new EventProducer(ringBuffer);
        while (true) {
            TimeUnit.MILLISECONDS.sleep(200);
            producer.sort(PredictDisruptor.generatePredictItem(10));
        }
    }

}
