package com.sdu.stream.topology.group;

import com.google.common.collect.Lists;
import com.sdu.stream.common.operation.impl.CycleTupleGenerator;
import com.sdu.stream.topology.group.bolt.SentenceSplitBolt;
import com.sdu.stream.topology.group.bolt.WordSumBolt;
import com.sdu.stream.common.FixedCycleSpout;
import com.sdu.stream.utils.Const;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;

/**
 * Storm Stream分组拓扑
 *
 * @author hanhan.zhang
 * */
public class StreamGroupTopology {

    public static void main(String[] args) {

        // builder
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        String spoutStreamId = "topology.flow.cycle.spout.stream";
        String spoutComponentName = "sentence.cycle.spout";
        boolean spoutStreamDirect = true;
        int spoutExecutorParallelism = 1;
        FixedCycleSpout cycleSpout = new FixedCycleSpout(spoutStreamId, spoutStreamDirect, new Fields("sentence"), new CycleTupleGenerator());
        topologyBuilder.setSpout (spoutComponentName, cycleSpout, spoutExecutorParallelism);

        // bolt
        String splitStreamId = "topology.flow.split.bolt.stream";
        String splitComponentName = "sentence.split.bolt";
        boolean splitStreamDirect = false;
        int splitExecutorParallelism = 2;
        // 默认:executor = task
        int splitBoltTask = 4;
        SentenceSplitBolt splitBolt = new SentenceSplitBolt(splitStreamId, splitStreamDirect);
        topologyBuilder.setBolt(splitComponentName, splitBolt, splitExecutorParallelism)
                        .setNumTasks(splitBoltTask)
                        .directGrouping(spoutComponentName, spoutStreamId);

        String sumComponentName = "word.sum.bolt";
        int sumExecutorParallelism = 2;
        WordSumBolt sumBolt = new WordSumBolt();
        topologyBuilder.setBolt(sumComponentName, sumBolt, sumExecutorParallelism)
                        .fieldsGrouping(splitComponentName, splitStreamId, new Fields("word"));

        Config config = new Config();
        config.setDebug(false);
        config.put(Const.SEPARATOR, " ");

        LocalCluster localCluster = new LocalCluster();

        // TopologyBuilder.createTopology()序列化各组件(属性必须可序列化)
        localCluster.submitTopology("flowTopology", config, topologyBuilder.createTopology());

    }

}
