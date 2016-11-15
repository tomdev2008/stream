package com.sdu.stream.topology.group.spout;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.storm.generated.Grouping;
import org.apache.storm.metric.api.CountMetric;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Fixed Cycle Spout
 *
 * @author hanhan.zhang
 * */
public class FixedCycleSpout implements IRichSpout {

    private static final Logger LOGGER = LoggerFactory.getLogger(FixedCycleSpout.class);

    // 是否为直接流
    private boolean _direct;

    // 流名称
    private String _streamId;

    private int _index;

    // key = msgId, value = sending tuple
    private Map<String, List<Object>> _pendingTuple;

    // tuple data
    private ArrayList<List<Object>> _sendTuple;

    private Fields _fields;

    private SpoutOutputCollector _collector;
    private CountMetric _sendMetric;
    private CountMetric _failMetric;

    // consume task set(下游消费者)
    private List<Integer> _consumeTaskIdList;

    public FixedCycleSpout(Fields _fields, ArrayList<List<Object>> _sendTuple) {
        this(Utils.DEFAULT_STREAM_ID, false, _fields, _sendTuple);
    }

    public FixedCycleSpout(String _streamId, boolean _direct, Fields _fields, ArrayList<List<Object>> _sendTuple) {
        this._streamId = _streamId;
        this._direct = _direct;
        this._fields = _fields;
        this._sendTuple = _sendTuple;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this._index = 0;
        _pendingTuple = Maps.newHashMap();

        // 统计信息
        this._sendMetric = context.registerMetric("cycle.spout.send.tuple.metric", new CountMetric(), 60);
        this._failMetric = context.registerMetric("cycle.spout.fail.tuple.metric", new CountMetric(), 60);
        this._collector = collector;

        // 直接流需要下游消费者
        if (this._direct) {
            this._consumeTaskIdList = Lists.newLinkedList();
            Map<String, Map<String, Grouping>> consumeTargets = context.getThisTargets();
            if (consumeTargets != null && !consumeTargets.isEmpty()) {
                // streamId = this._streamId
                consumeTargets.forEach((streamId, target) -> {
                    if (target != null && !target.isEmpty()) {
                        // componentId = consume target component Id
                        target.forEach((componentId, group) -> {
                            if (group.is_set_direct()) {
                                this._consumeTaskIdList.addAll(context.getComponentTasks(componentId));
                            }
                        });
                    }
                });
            }
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        this._sendMetric.incr();
        if (this._index == _sendTuple.size()) {
            this._index = 0;
        }
        String msgId = UUID.randomUUID().toString();
        List<Object> tuple = this._sendTuple.get(this._index++);
        sendTuple(msgId, tuple);
    }

    @Override
    public void ack(Object msgId) {
        String msgIdStr = (String) msgId;
        LOGGER.debug("ack tuple message id {} .", msgIdStr);
        this._pendingTuple.remove(msgIdStr);
    }

    @Override
    public void fail(Object msgId) {
        this._failMetric.incr();
        String msgIdStr = (String) msgId;
        LOGGER.debug("fail tuple message id {} .", msgIdStr);
        sendTuple(msgIdStr, this._pendingTuple.get(msgIdStr));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(this._streamId, this._direct, this._fields);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    protected void sendTuple(String msgId, List<Object> tuple) {
        this._pendingTuple.put(msgId, tuple);
        if (this._direct) {
            if (this._consumeTaskIdList == null || this._consumeTaskIdList.isEmpty()) {
                throw new IllegalStateException("direct task is empty !");
            }
            this._consumeTaskIdList.forEach(taskId ->
                    this._collector.emitDirect(taskId, this._streamId, tuple, msgId));
        } else {
            this._collector.emit(this._streamId, tuple, msgId);
        }
    }
}
