package com.sdu.stream.topology.flow.router.bolt;

import org.apache.logging.log4j.util.Strings;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * Stream Print Bolt
 *
 * @author hanhan.zhang
 * */
public class OutputBolt extends BaseRichBolt {

    private OutputCollector _collector;

    private String _interestStream;

    public OutputBolt(String _interestStream) {
        this._interestStream = _interestStream;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this._collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String str = input.getString(0);
        if (Strings.isEmpty(str)) {
            return;
        }
        System.out.println("Interest Stream '" + _interestStream + "', Receive Input : " + str);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
