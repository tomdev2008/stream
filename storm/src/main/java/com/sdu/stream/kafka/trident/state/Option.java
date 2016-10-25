package com.sdu.stream.kafka.trident.state;

import lombok.Getter;
import lombok.Setter;
import org.apache.storm.trident.state.Serializer;
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

    private Jedis jedis;

    private int localCacheSize;
}
