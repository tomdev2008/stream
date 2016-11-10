package com.sdu.stream.communicate.thread.disruptor;

import com.lmax.disruptor.RingBuffer;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * {@link RingBuffer} save event
 *
 * @author hanhan.zhang
 * */
@Setter
@Getter
@ToString
public class NumEvent {

    private int value;

}
