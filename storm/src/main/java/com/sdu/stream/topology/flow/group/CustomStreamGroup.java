package com.sdu.stream.topology.flow.group;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.util.List;

/**
 * Custom Tuple Stream Group
 *
 * @author hanhan.zhang
 * */
public class CustomStreamGroup implements CustomStreamGrouping {

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {

    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        return null;
    }
}
