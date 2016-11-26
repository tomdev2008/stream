package com.sdu.stream.common.operation;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.windowing.TupleWindow;

import java.io.Serializable;

/**
 * Tuple Process
 *
 * @author hanhan.zhang
 * */
public interface TupleOperation<T> extends Serializable {

    public void process(OutputCollector collector, T tuple);

}
