package com.sdu.stream.kafka.trident.state;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.StateType;
import org.apache.storm.trident.state.map.IBackingMap;

import java.util.List;
import java.util.Map;

/**
 * @author hanhan.zhang
 * */
public class RedisMapState<T> implements IBackingMap<T>{

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        return null;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> values) {

    }

    protected static class RedisStateFactory implements StateFactory {
        // state enum
        private StateType _stateType;

        public RedisStateFactory(StateType stateType) {
            this._stateType = stateType;
        }

        @Override
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            return null;
        }
    }

    protected static class RedisState {

    }
}
