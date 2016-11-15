package com.sdu.stream.transaction.original.bolt;

import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.trident.topology.BatchInfo;
import org.apache.storm.trident.topology.ITridentBatchBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * Trident Batch Bolt(Sentence Split). See{@link org.apache.storm.trident.spout.TridentSpoutExecutor}
 *
 * @author hanhan.zhang
 * */
public class SentenceSplitBolt implements ITridentBatchBolt {

    private Fields _field;

    private String _streamId;

    private boolean _direct;

    private BatchOutputCollector _collector;

    public SentenceSplitBolt(Fields _field) {
        this(_field, Utils.DEFAULT_STREAM_ID, false);
    }

    public SentenceSplitBolt(Fields _field, String _streamId, boolean _direct) {
        this._field = _field;
        this._streamId = _streamId;
        this._direct = _direct;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector) {
        this._collector = collector;
    }

    @Override
    public void execute(BatchInfo batchInfo, Tuple tuple) {

    }

    @Override
    public void finishBatch(BatchInfo batchInfo) {

    }

    @Override
    public Object initBatchState(String batchGroup, Object batchId) {
        return null;
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(this._streamId, this._direct, this._field);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
