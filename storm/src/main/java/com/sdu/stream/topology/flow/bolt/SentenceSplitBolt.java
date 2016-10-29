package com.sdu.stream.topology.flow.bolt;

import com.google.common.base.Strings;
import com.sdu.stream.utils.Const;
import org.apache.storm.metric.api.CountMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import scala.util.parsing.combinator.testing.Str;

import java.util.Map;

/**
 * Sentence Split Bolt
 *
 * @author hanhan.zhang
 * */
public class SentenceSplitBolt implements IRichBolt {

    private OutputCollector _collector;

    private CountMetric _ackMetric;

    private CountMetric _failMetric;

    private String _separator;

    private int _taskId;

    private boolean _direct;

    private String _streamId;

    public SentenceSplitBolt(String _streamId, boolean _direct) {
        this._streamId = _streamId;
        this._direct = _direct;
    }

    /**
     * @param context
     *          1: Register Metric
     *          2: Next Bolt Message
     * @param collector (thread-safe)
     *          1: Emit Tuple
     *          2: Ack/Fail Tuple
     * */
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this._collector = collector;
        // register metric for monitor
        this._ackMetric = context.registerMetric("sentence.split.ack.metric", new CountMetric(), 60);
        this._failMetric = context.registerMetric("sentence.split.fail.metric", new CountMetric(), 60);
        this._taskId = context.getThisTaskId();

        this._separator = (String) stormConf.get(Const.SEPARATOR);
    }

    @Override
    public void execute(Tuple input) {
        try {
            String sentence = input.getString(0);
            if (Strings.isNullOrEmpty(sentence)) {
                return;
            }
            String []fields = sentence.split(_separator);
            for (String field : fields) {
                if (this._direct) {
                    this._collector.emitDirect(this._taskId, _streamId, input, new Values(field, 1));
                } else {
                    this._collector.emit(this._streamId, input, new Values(field, 1));
                }
            }
            this._collector.ack(input);
            this._ackMetric.incr();
        } catch (Exception e) {
            this._collector.fail(input);
            this._failMetric.incr();
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(this._streamId, this._direct, new Fields("word", "count"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
