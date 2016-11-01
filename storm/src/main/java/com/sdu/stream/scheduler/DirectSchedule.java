package com.sdu.stream.scheduler;

import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.Topologies;

import java.util.Map;

/**
 * Storm Task Scheduler(http://blog.csdn.net/androidlushangderen/article/details/46834893)
 *
 * @author hanhan.zhang
 * */
public class DirectSchedule implements IScheduler {

    @Override
    public void prepare(Map conf) {

    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {

    }
}
