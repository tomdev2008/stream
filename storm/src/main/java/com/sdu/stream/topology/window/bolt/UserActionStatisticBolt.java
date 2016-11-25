package com.sdu.stream.topology.window.bolt;

import com.sdu.stream.topology.window.operation.WindowTupleOperation;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IWindowedBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.windowing.TupleWindow;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * see {@link BaseWindowedBolt}
 *
 *
    |---------------------------------------------------------------------|
    | WindowLength = 7, SlideLength = 1                                   |                             |
    |      tuple1, tuple2, tuple3, tuple4, tuple5, tuple6, tuple7, tuple8 |
    |      tuple1, tuple2, tuple3, tuple4, tuple5, tuple6, tuple7, tuple8 |
    |        |        |                                       |       |   |
    |        |________|____________Window1____________________|       |   |
    |                 |_____________________Window2___________________|   |
    |---------------------------------------------------------------------|
 *
 * @author hanhan.zhang
 * */
public class UserActionStatisticBolt implements IWindowedBolt {

    // Bolt Config
    protected final transient Map<String, Object> _windowConfig;

    private WindowTupleOperation _operation;

    public UserActionStatisticBolt(WindowTupleOperation windowTupleOperation) {
        this._windowConfig = new HashMap<>();
        this._operation = windowTupleOperation;
    }

    // 指定Window长度
    private UserActionStatisticBolt withWindowLength(Count count) {
        this._windowConfig.put(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT, count._value);
        return this;
    }

    // 指定Window在duration内长度
    private UserActionStatisticBolt withWindowLength(Duration duration) {
        this._windowConfig.put(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS, duration._value);
        return this;
    }

    // 指定Window每次滑动大小
    private UserActionStatisticBolt withWindowSlideLength(Count count) {
        this._windowConfig.put(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_COUNT, count);
        return this;
    }

    private UserActionStatisticBolt withWindowSlideLength(Duration duration) {
        this._windowConfig.put(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS, duration._value);
        return this;
    }

    public UserActionStatisticBolt withWindow(Count windowLength, Count slidingInterval) {
        return withWindowLength(windowLength).withWindowSlideLength(slidingInterval);
    }

    public UserActionStatisticBolt withWindow(Duration windowLength, Duration slidingInterval) {
        return withWindowLength(windowLength).withWindowSlideLength(slidingInterval);
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(TupleWindow inputWindow) {
        this._operation.process(inputWindow);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return this._windowConfig;
    }

    public static final class Duration {
        private int _value;

        public Duration(int value, TimeUnit timeUnit) {
            this._value = (int) timeUnit.toMillis(value);
        }
    }

    public static final class Count {
        public final int _value;

        public Count(int value) {
            this._value = value;
        }
    }
}
