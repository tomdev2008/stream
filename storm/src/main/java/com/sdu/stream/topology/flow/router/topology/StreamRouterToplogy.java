package com.sdu.stream.topology.flow.router.topology;

import com.google.common.collect.Lists;
import com.sdu.stream.topology.flow.router.bolt.MultiStreamBolt;
import com.sdu.stream.topology.flow.router.bolt.StreamPrintBolt;
import com.sdu.stream.topology.flow.router.help.StreamDesc;
import com.sdu.stream.topology.flow.spout.FixedCycleSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.List;

/**
 * Tuple Stream Router Topology
 *
 * @author hanhan.zhang
 * */
public class StreamRouterToplogy {

    public static void main(String[] args) {
        // send tuple
        List<Object>[]tuple = new List[] {new Values("request: request log from ip 192.12.1.67"),
                                            new Values("request: request log from ip 192.124.13.69"),
                                            new Values("response: response log from ip 80.127.13.69"),
                                            new Values("response: response log from ip 10.126.13.69")};



        //stream name
        String spoutStreamId = "topology.spout.stream";

        String requestStreamId = "topology.request.stream";
        String requestFlag = "request";
        String responseStreamId = "topology.response.stream";
        String responseFlag = "response";
        List<StreamDesc> streamDescs = Lists.newArrayList(StreamDesc.builder()
                                                                    .streamId(requestStreamId)
                                                                    .flag(requestFlag)
                                                                    .direct(false)
                                                                    .fields(new Fields("log"))
                                                                    .build(),
                                                          StreamDesc.builder()
                                                                    .streamId(responseStreamId)
                                                                    .flag(responseFlag)
                                                                    .direct(false)
                                                                    .fields(new Fields("log"))
                                                                  .build());

        // spout
        FixedCycleSpout cycleSpout = new FixedCycleSpout(spoutStreamId, "log", false, tuple);

        // bolt
        MultiStreamBolt multiStreamBolt = new MultiStreamBolt(streamDescs);
        StreamPrintBolt requestPrintBolt = new StreamPrintBolt(requestStreamId, false);
        StreamPrintBolt responsePrintBolt = new StreamPrintBolt(responseStreamId, true);

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout ("log.cycle.spout", cycleSpout, 1);

        topologyBuilder.setBolt("multiple.stream.bolt", multiStreamBolt, 1)
                        .shuffleGrouping("log.cycle.spout", spoutStreamId);

        topologyBuilder.setBolt("request.print.bolt", requestPrintBolt, 1)
                        .shuffleGrouping("multiple.stream.bolt", requestStreamId);
        topologyBuilder.setBolt("response.print.bolt", responsePrintBolt, 1)
                        .shuffleGrouping("multiple.stream.bolt", responseStreamId);

        Config config = new Config();
        config.setDebug(false);

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("multiSteamRouteTopology", config, topologyBuilder.createTopology());

    }

}
