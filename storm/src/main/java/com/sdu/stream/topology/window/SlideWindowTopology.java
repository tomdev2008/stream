package com.sdu.stream.topology.window;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sdu.stream.common.FixedCycleSpout;
import com.sdu.stream.common.operation.impl.CycleTupleGenerator;
import com.sdu.stream.topology.window.bolt.UserActionBolt;
import com.sdu.stream.topology.window.bolt.UserActionStatisticBolt;
import com.sdu.stream.common.operation.TupleOperation;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.WindowedBoltExecutor;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Storm滑动窗口(如：每隔5秒计算过去20秒的用户的浏览量)
 *
 * Note：
 *
 *  |------------------------------------------------------------------------------------------------------
    |1：Storm Window Tuple
    | 1' 基于Tuple Size
    | 2' 基于Tuple Timestamp(需要声明Tuple中Timestamp字段)
    |
    |2：{@link WindowedBoltExecutor}
    | 1' {@link TriggerPolicy}负责触发窗口的滑动
    | 2' {@link EvictionPolicy}负责触发窗口的生成
    | 3' {@link WindowLifecycleListener}负责Tuple处理(失效Event确认Tuple,处理)
    | 4' {@link WindowManager}管理窗口(窗口滑动、窗口生成、窗口事件处理)
    |------------------------------------------------------------------------------------------------------

 ， * @author hanhan.zhang
 * */
public class SlideWindowTopology {

    private static final Logger LOGGER = LoggerFactory.getLogger(SlideWindowTopology.class);

    public static void main(String[] args) {

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        ArrayList<List<Object>> dataSource = Lists.newArrayList(new Values("256901", "glance"),
                new Values("256902", "glance"),
                new Values("256903", "click"),
                new Values("256905", "click"),
                new Values("256901", "order"),
                new Values("256902", "order"),
                new Values("256903", "click"),
                new Values("256905", "click"),
                new Values("256906", "glance"));

        // spout
        String spoutComponentName = "cycle.spout";
        Fields spoutField = new Fields("userId", "actionType");
        String spoutStream = "fixed.cycle.spout.stream";
        FixedCycleSpout cycleSpout = new FixedCycleSpout(spoutStream, false, spoutField, new CycleTupleGenerator(dataSource));
        topologyBuilder.setSpout(spoutComponentName, cycleSpout, 1);

        // window bolt
        String actionComponentName = "user.action.bolt";
        String actionStream = "user.action.window.stream";
        Fields actionField = new Fields("actionType", "number");
        UserActionBolt userActionBolt = new UserActionBolt(actionStream, false, actionField);
        topologyBuilder.setBolt(actionComponentName, userActionBolt, 1)
                       .shuffleGrouping(spoutComponentName, spoutStream);

        // window bolt
        TupleOperation<TupleWindow> operation = (collector, windowTuple) -> {
            List<Tuple> total = windowTuple.get();
            Map<String, Integer> sumMap = Maps.newHashMap();
            total.forEach(tuple -> {
                String actionType = tuple.getStringByField("actionType");
                int num = tuple.getIntegerByField("number");
                Integer oldNumber = sumMap.get(actionType);
                if (oldNumber == null) {
                    oldNumber = 0;
                }
                oldNumber += num;
                sumMap.put(actionType, oldNumber);
            });
            sumMap.forEach((actionType, num) -> LOGGER.info("在过去20MS内，{} Action数据是：{}", actionType, num));
        };
        String statisticComponentName = "user.action.statistic.bolt";
        BaseWindowedBolt.Count windowLength = new BaseWindowedBolt.Count(10);
        BaseWindowedBolt.Count slideLength = new BaseWindowedBolt.Count(10);
        UserActionStatisticBolt  statisticBolt = (UserActionStatisticBolt) new UserActionStatisticBolt(operation)
                                                        .withWindow(windowLength, slideLength);

        topologyBuilder.setBolt(statisticComponentName, statisticBolt, 1)
                       .fieldsGrouping(actionComponentName, actionStream, new Fields("actionType"));

        // 提交作业
        Config config = new Config();

        String topologyName = "windowTopology";
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology(topologyName, config, topologyBuilder.createTopology());
    }

}
