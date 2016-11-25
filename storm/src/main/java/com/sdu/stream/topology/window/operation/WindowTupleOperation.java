package com.sdu.stream.topology.window.operation;

import org.apache.storm.windowing.TupleWindow;

import java.io.Serializable;

/**
 * 窗口内Tuple计算
 *
 * @author hanhan.zhang
 * */
public interface WindowTupleOperation<R> extends Serializable{

    public R process(TupleWindow tupleWindow);

}
