package com.sdu.stream.topology.flow;

import com.sdu.stream.topology.flow.bolt.SentenceSplitBolt;
import com.sdu.stream.topology.flow.bolt.WordSumBolt;
import com.sdu.stream.topology.flow.spout.FixedCycleSpout;
import com.sdu.stream.utils.Const;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.List;

/**
 * Tuple Split-Flow Topology
 *
 * @author hanhan.zhang
 * */
public class FlowTopology {

    public static void main(String[] args) {

        // config storm.yaml
        System.setProperty("storm.conf.file", "storm/storm.yaml");

//        Utils.readStormConfig();

        // send tuple
        List<Object> []tuple = new List[] {new Values("the cow jumped over the moon"),
                                            new Values("the man went to the store and bought some candy"),
                                            new Values("four score and seven years ago"),
                                            new Values("how many apples can you eat")};


        //stream name
        String spoutStreamId = "topology.flow.cycle.spout.stream";
        String splitStreamId = "topology.flow.split.bolt.stream";

        // spout
        FixedCycleSpout cycleSpout = new FixedCycleSpout(spoutStreamId, "sentence", true, tuple);

        // bolt
        SentenceSplitBolt splitBolt = new SentenceSplitBolt(splitStreamId, false);
        WordSumBolt sumBolt = new WordSumBolt();

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout ("sentence.cycle.spout", cycleSpout, 1);

        topologyBuilder.setBolt("sentence.split.bolt", splitBolt, 1)
                        .directGrouping("sentence.cycle.spout", spoutStreamId);

        topologyBuilder.setBolt("word.sum.bolt", sumBolt, 3)
                        .fieldsGrouping("sentence.split.bolt", splitStreamId, new Fields("word"));

        Config config = new Config();
        config.setDebug(true);
        config.put(Const.SEPARATOR, " ");

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("flowTopology", config, topologyBuilder.createTopology());

    }

}
