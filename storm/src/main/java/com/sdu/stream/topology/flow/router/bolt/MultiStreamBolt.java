package com.sdu.stream.topology.flow.router.bolt;

import com.sdu.stream.topology.flow.router.help.StreamDesc;
import org.apache.logging.log4j.util.Strings;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;

/**
 * Multiple Stream Bolt
 *
 * @author hanhan.zhang
 * */
public class MultiStreamBolt extends BaseRichBolt {

    private OutputCollector _collector;

    private boolean _direct;

    private List<StreamDesc> streamDescList;

    private Fields _fields;

    public MultiStreamBolt(boolean _direct, List<StreamDesc> streamDescList, Fields _fields) {
        this._direct = _direct;
        this.streamDescList = streamDescList;
        this._fields = _fields;
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
        streamDescList.forEach(streamDesc -> {
            if (streamDesc.interest(str)) {
                this._collector.emit(input, new Values(str));
            }
        });
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        streamDescList.forEach(streamDesc ->
            declarer.declareStream(streamDesc.getStreamId(), this._direct, this._fields)
        );
    }
}
