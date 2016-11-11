package com.sdu.stream.communicate.thread.disruptor.share;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.sdu.stream.communicate.thread.disruptor.process.batch.PredictDisruptor;
import com.sdu.stream.communicate.thread.disruptor.process.batch.handler.FeatureLoader;
import com.sdu.stream.communicate.thread.disruptor.process.batch.handler.ResponseHandler;
import com.sdu.stream.communicate.thread.disruptor.process.batch.handler.ScoreHandler;
import com.sdu.stream.communicate.thread.disruptor.util.PredictResult;
import com.sdu.stream.communicate.thread.disruptor.util.PredictTracker;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 *
 *
 * @author hanhan.zhang
 * */
public class PredictService {

    // Event Producer
    private EventProducer _producer;

    public PredictService() {
        this.init();
    }

    protected void init() {
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
        _producer = new EventProducer(ringBuffer);
    }

    public PredictResult predict(List<Integer> predictItemList) throws Exception {
        long sequence = this._producer.sort(predictItemList);
        return PredictTracker.getPredictResult(sequence, true);
    }

    public static void main(String[] args) throws Exception {
        PredictService predictService = new PredictService();
        while (true) {
            TimeUnit.SECONDS.sleep(5);
            PredictResult result = predictService.predict(PredictDisruptor.generatePredictItem(5));
            System.out.println("请求序号:" + result.getSequence() + ", 模型排序耗时:" + result.getCost() + "ms !");
            System.out.println("排序结果:" + result.getScoreMap());
        }
    }

}
