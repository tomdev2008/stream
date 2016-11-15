package com.sdu.stream.transaction.original;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.sdu.stream.transaction.original.bolt.SentenceSplitBolt;
import com.sdu.stream.transaction.original.spout.FixedCycleTridentSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.shade.com.google.common.collect.Lists;
import org.apache.storm.trident.topology.TridentTopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Trident Topology
 *
 * @author hanhan.zhang
 * */
public class OriginalTridentTopology {



    public static void main(String[] args) {
        // builder
        TridentTopologyBuilder builder = new TridentTopologyBuilder();

        // spout
        Fields spoutFields = new Fields("sentence");
        ArrayList<List<Object>> sentenceTuple = Lists.newArrayList(new Values("the cow jumped over the moon"),
                                                                new Values("the man went to the store and bought some candy"),
                                                                new Values("four score and seven years ago"),
                                                                new Values("how many apples can you eat"));
        FixedCycleTridentSpout tridentSpout = new FixedCycleTridentSpout(spoutFields, sentenceTuple);
        String spoutComponentName = "fixed.cycle.trident.spout";
        String spoutStreamName = "fixed.trident.spout.stream";
        // 事务组件特有,对应Zookeeper中路径(保存元数据)
        String spoutTxStateId = "fixed.spout.tx.state";
        String spoutBatchGroup = "fixed.spout.batch.stream.group";
        int spoutParallelism = 1;
        builder.setSpout(spoutComponentName, spoutStreamName, spoutTxStateId, tridentSpout, spoutParallelism, spoutBatchGroup);

        // bolt
        String splitComponentName = "fixed.split.trident.batch.bolt";
        String splitStreamName = "fixed.split.blot.stream";
        String splitBatchGroup = "fixed.split.bolt.stream.batch.group";
        int boltSplitParallelism = 1;
        // 表明哪些节点与该节点有联系(该Bolt节点将收听这些节点所对应的MasterBatchCoordinator节点的$commit流)
        Set<String> committerBatches = Sets.newHashSet(spoutBatchGroup);

        Map<String, String> batchGroups = Maps.newHashMap();
        batchGroups.put(splitStreamName, splitBatchGroup);
        SentenceSplitBolt splitBolt = new SentenceSplitBolt(new Fields("word", "number"));
        builder.setBolt(splitComponentName, splitBolt, boltSplitParallelism, committerBatches, batchGroups)
                .shuffleGrouping(spoutComponentName, spoutStreamName);

        // config
        Config config = new Config();
        config.setDebug(true);

        LocalCluster localCluster = new LocalCluster();
        String topologyName = "tridentTopology";
        localCluster.submitTopology(topologyName, config, builder.buildTopology());
    }

}
