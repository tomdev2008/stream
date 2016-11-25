package com.sdu.stream.topology.window.bolt;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sdu.stream.utils.CollectionUtil;
import com.sdu.stream.utils.MapUtil;
import org.apache.storm.generated.Grouping;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.trident.partition.GlobalGrouping;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * User Action Bolt
 *
 * @author hanhan.zhang
 * */
public class UserActionBolt extends BaseWindowedBolt {

    private OutputCollector _collector;

    private String _stream;

    private boolean _direct;

    private Fields _fields;

    // consume task
    private AtomicInteger _curr;
    private List<Integer> _consumeTaskList;

    public UserActionBolt(String _stream, boolean _direct, Fields _fields) {
        this._stream = _stream;
        this._direct = _direct;
        this._fields = _fields;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this._collector = collector;
        if (this._direct) {
            this._consumeTaskList = Lists.newArrayList();
            this._curr = new AtomicInteger(0);
            Map<String, Map<String, Grouping>> consumeComponentMap = context.getThisTargets();
            consumeComponentMap.forEach((streamId, componentMap) ->
                componentMap.forEach((component, grouping) -> {
                    this._consumeTaskList.addAll(context.getComponentTasks(component));
                })
            );
        }
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        List<Tuple> newTuple = inputWindow.getNew();
        Map<Tuple, List<Object>> sendTupleMap = getSendTuple(newTuple);
        if (MapUtil.isNotEmpty(sendTupleMap)) {
            sendTupleMap.forEach((tuple, sendTuple) -> {
                try {
                    if (this._direct) {
                        int index = this._curr.getAndIncrement();
                        int taskId = index % this._consumeTaskList.size();
                        this._collector.emitDirect(taskId, this._stream, tuple, sendTuple);
                    } else {
                        this._collector.emit(this._stream, tuple, sendTuple);
                    }
                } catch (Exception e) {
                    this._collector.fail(tuple);
                }
            });
        }
        this.ack(inputWindow.get());
    }

    protected Map<Tuple, List<Object>> getSendTuple(List<Tuple> tupleList) {
        if (CollectionUtil.isEmpty(tupleList)) {
            return null;
        }
        Map<Tuple, List<Object>> map = Maps.newLinkedHashMapWithExpectedSize(tupleList.size());
        tupleList.forEach(tuple -> {
            String type = tuple.getStringByField("actionType");
            map.put(tuple, new Values(type, 1));
        });
        return map;
    }

    protected void ack(List<Tuple> tupleList) {
        if (CollectionUtil.isNotEmpty(tupleList)) {
            tupleList.forEach(tuple -> this._collector.ack(tuple));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(this._stream, this._direct, this._fields);
    }

}
