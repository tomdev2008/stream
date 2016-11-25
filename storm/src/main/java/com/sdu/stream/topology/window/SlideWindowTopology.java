package com.sdu.stream.topology.window;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sdu.stream.topology.group.spout.FixedCycleSpout;
import com.sdu.stream.topology.window.bolt.UserActionBolt;
import com.sdu.stream.topology.window.bolt.UserActionStatisticBolt;
import com.sdu.stream.topology.window.operation.WindowTupleOperation;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.IWindowedBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.WindowedBoltExecutor;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Storm滑动窗口
 *
 * Note：
 *  1 ---> 每隔5秒钟计算最近20秒内UV
 *  2 ---> {@link TopologyBuilder#setBolt(String, IWindowedBolt)}通过{@link WindowedBoltExecutor}
 *         对{@link IWindowedBolt}包装
 *
 * @author hanhan.zhang
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
        FixedCycleSpout cycleSpout = new FixedCycleSpout(spoutStream, false, spoutField, dataSource);
        topologyBuilder.setSpout(spoutComponentName, cycleSpout, 1);

        // window bolt
        String actionComponentName = "user.action.bolt";
        String actionStream = "user.action.window.stream";
        Fields actionField = new Fields("actionType", "number");
        UserActionBolt userActionBolt = new UserActionBolt(actionStream, false, actionField);
        topologyBuilder.setBolt(actionComponentName, userActionBolt, 1)
                       .shuffleGrouping(spoutComponentName, spoutStream);

        // window bolt
        WindowTupleOperation<Void> operation = (windowTuple) -> {
            List<Tuple> total = windowTuple.get();
            Map<String, Integer> sumMap = Maps.newHashMap();
            total.forEach(tuple -> {
                String actionType = tuple.getStringByField("actionType");
                int num = tuple.getByteByField("number");
                Integer oldNumber = sumMap.get(actionType);
                if (oldNumber == null) {
                    oldNumber = 0;
                }
                oldNumber += num;
                sumMap.put(actionType, oldNumber);
            });
            sumMap.forEach((actionType, num) -> LOGGER.info("在过去20MS内，{} Action数据是：{}", actionType, num));
            return null;
        };
        String statisticComponentName = "user.action.statistic.bolt";
//        UserActionStatisticBolt.Duration windowLength = new UserActionStatisticBolt.Duration(20, TimeUnit.MILLISECONDS);
//        UserActionStatisticBolt.Duration slideLength = new UserActionStatisticBolt.Duration(10, TimeUnit.MILLISECONDS);
        UserActionStatisticBolt.Count windowLength = new UserActionStatisticBolt.Count(10);
        UserActionStatisticBolt.Count slideLength = new UserActionStatisticBolt.Count(5);
        UserActionStatisticBolt  statisticBolt = new UserActionStatisticBolt(operation).withWindow(windowLength, slideLength);

        topologyBuilder.setBolt(statisticComponentName, statisticBolt, 1)
                       .fieldsGrouping(actionComponentName, actionStream, new Fields("actionType"));

        // 提交作业
        Config config = new Config();

        String topologyName = "windowTopology";
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology(topologyName, config, topologyBuilder.createTopology());
    }

}
