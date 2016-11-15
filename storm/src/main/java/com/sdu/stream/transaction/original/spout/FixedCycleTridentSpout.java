package com.sdu.stream.transaction.original.spout;

import com.google.common.collect.Maps;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.ITridentSpout;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.apache.storm.trident.topology.TridentTopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Trident Spout
 *
 * @author hanhan.zhang
 * */
public class FixedCycleTridentSpout implements ITridentSpout {

    private static final Logger LOGGER = LoggerFactory.getLogger(FixedCycleTridentSpout.class);

    private Fields _fields;

    private ArrayList<List<Object>> _outputTuple;

    public FixedCycleTridentSpout(Fields _fields, ArrayList<List<Object>> _outputTuple) {
        this._fields = _fields;
        this._outputTuple = _outputTuple;
    }

    @Override
    public BatchCoordinator getCoordinator(String txStateId, Map conf, TopologyContext context) {
        int taskSize = context.getComponentTasks(TridentTopologyBuilder.spoutIdFromCoordinatorId(context.getThisComponentId())).size();
        return new FixedCycleTridentCoordinator(taskSize);
    }

    @Override
    public Emitter getEmitter(String txStateId, Map conf, TopologyContext context) {
        return new FixedCycleTridentEmitter(context.getThisTaskIndex());
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return _fields;
    }

    /**
     * coordinator spout to send tuple
     * executor on master node
     * */
    protected class FixedCycleTridentCoordinator implements BatchCoordinator<Map<Integer, List<List<Object>>>> {

        // tuple partition = executor number
        private int _numPartitions;

        // key = 事务ID, value = 事务ID对应的起始索引
        private Map<Long, Integer> _txIndices = new HashMap();

        private int _emittedIndex = 0;

        private int _batchSize = 3;

        private long _commintTxid;

        public FixedCycleTridentCoordinator(int _numPartitions) {
            this._numPartitions = _numPartitions;
        }

        public Map<Integer, List<List<Object>>> initializeTransaction(long txid, Map<Integer, List<List<Object>>> prevMetadata, Map<Integer, List<List<Object>>> currMetadata) {
            if (currMetadata != null) {
                return currMetadata;
            }
            // make sure every executor can receive tuple
            Map<Integer, List<List<Object>>> partitionTupleMap = Maps.newHashMap();
            this._txIndices.put(txid, this._emittedIndex);
            for (int i = 0; i < this._numPartitions; ++i) {
                List<List<Object>> tupleList = new ArrayList<>(this._batchSize);
                for (int j = this._emittedIndex; j < this._batchSize; ++j) {
                    tupleList.add(_outputTuple.get(j % _outputTuple.size()));
                }
                partitionTupleMap.put(i, tupleList);
                this._emittedIndex += this._batchSize;
            }
            return partitionTupleMap;
        }

        @Override
        public void success(long txid) {
            this._txIndices.remove(txid);
            this._commintTxid = txid;
        }

        @Override
        public boolean isReady(long txid) {
            return txid > this._commintTxid;
        }

        @Override
        public void close() {

        }
    }

    /**
     * execute on multiple node
     * send tuple to bolt
     * */
    protected class FixedCycleTridentEmitter implements Emitter<Map<Integer, List<List<Object>>>> {

        // executor index
        private int _index;

        public FixedCycleTridentEmitter(int _index) {
            this._index = _index;
        }

        @Override
        public void emitBatch(TransactionAttempt tx, Map<Integer, List<List<Object>>> coordinatorMeta, TridentCollector collector) {
            List<List<Object>> tupleList = coordinatorMeta.get(this._index);
            tupleList.forEach(tuple -> collector.emit(tuple));
        }

        @Override
        public void success(TransactionAttempt tx) {

        }

        @Override
        public void close() {

        }
    }
}
