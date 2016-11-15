package com.sdu.stream.transaction.original.spout;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.transactional.ITransactionalSpout;

import java.util.Map;

/**
 * Transaction Spout
 *
 * @author hanhan.zhang
 * */
public class FixedCycleTranactionSpout<T> implements ITransactionalSpout<T> {

    @Override
    public Coordinator<T> getCoordinator(Map conf, TopologyContext context) {
        return null;
    }

    @Override
    public Emitter<T> getEmitter(Map conf, TopologyContext context) {
        return null;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
