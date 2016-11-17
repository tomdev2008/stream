package com.sdu.stream.topology.hook.task;

import org.apache.storm.hooks.ITaskHook;
import org.apache.storm.hooks.info.*;
import org.apache.storm.task.TopologyContext;

import java.io.Serializable;
import java.util.Map;

/**
 * Storm Task监控
 *
 * @author hanhan.zhang
 * */
public class TaskMonitorHook implements ITaskHook, Serializable {

    @Override
    public void prepare(Map conf, TopologyContext context) {

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void emit(EmitInfo info) {

    }

    @Override
    public void spoutAck(SpoutAckInfo info) {

    }

    @Override
    public void spoutFail(SpoutFailInfo info) {

    }

    @Override
    public void boltExecute(BoltExecuteInfo info) {

    }

    @Override
    public void boltAck(BoltAckInfo info) {

    }

    @Override
    public void boltFail(BoltFailInfo info) {

    }
}
