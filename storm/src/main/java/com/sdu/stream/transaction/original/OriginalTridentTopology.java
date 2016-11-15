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
        String spoutStreamName = "fixed.spout.trident.stream";
        String spoutTxStateId = "fixed.spout.tx.state";
        String spoutBatchGroup = "fixed.spout.batch";
        int spoutParallelism = 1;
        builder.setSpout(spoutComponentName, spoutStreamName, spoutTxStateId, tridentSpout, spoutParallelism, spoutBatchGroup);

        // bolt
        String splitComponentName = "fixed.split.trident.batch.bolt";
        int boltSplitParallelism = 1;
        Set<String> commitBatch = Sets.newHashSet();
        Map<String, String> batchGroups = Maps.newHashMap();
        SentenceSplitBolt splitBolt = new SentenceSplitBolt(new Fields("word", "number"));
        builder.setBolt(splitComponentName, splitBolt, boltSplitParallelism, commitBatch, batchGroups)
                .shuffleGrouping(splitComponentName, spoutStreamName);

        // config
        Config config = new Config();

        LocalCluster localCluster = new LocalCluster();
        String topologyName = "original.trident.topology";
        localCluster.submitTopology(topologyName, config, builder.buildTopology());
    }

}
