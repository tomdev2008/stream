package com.sdu.stream.trident.simple.state;

import lombok.Getter;
import lombok.Setter;
import org.apache.storm.trident.state.Serializer;
import org.apache.storm.trident.state.StateType;
import redis.clients.jedis.Jedis;

import java.io.Serializable;

/**
 * Redis Operation Option
 *
 * @author hanhan.zhang
 * */
@Getter
@Setter
public class Option<T> implements Serializable {

    private Serializer<T> serializer = null;

    private int localCacheSize;

    private StateType stateType;
}
