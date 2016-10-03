package com.sdu.stream.kafka.simple.topology;

import com.sdu.stream.kafka.simple.bolt.SentenceSplitBaseBolt;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.topology.TopologyBuilder;

/**
 * 拓扑构建
 *
 * @author hanhan.zhang
 * */
public class KafkaTopology {

    public final void start(String zkRoot, String topic, String id, String topologyName) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        /**storm config*/
        Config config = new Config();
        // 设置worker(JVM进程数)
        config.setNumWorkers(5);
        // 5秒tuple未确认,认为消息失败
        config.setMessageTimeoutSecs(5);
        // ACK(实质为Bolt)
        config.setNumAckers(3);
        config.setMaxSpoutPending(1000);

        /**kafka spout config*/
        BrokerHosts zkHost = new ZkHosts("127.0.0.1");
        SpoutConfig spoutConfig = new SpoutConfig(zkHost, topic, zkRoot, id);

        /**topology build*/
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout", new KafkaSpout(spoutConfig), 2);
        builder.setBolt("splitBolt", new SentenceSplitBaseBolt(), 2).shuffleGrouping("kafkaSpout");

        /**submit topology*/
        StormSubmitter.submitTopology(topologyName, config, builder.createTopology());
    }

    public static void main(String[] args) {

    }
}
