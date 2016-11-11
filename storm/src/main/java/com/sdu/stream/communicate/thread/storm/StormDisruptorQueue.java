package com.sdu.stream.communicate.thread.storm;

import com.lmax.disruptor.*;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.ProducerType;
import com.sdu.stream.communicate.thread.storm.listener.ConsumerListener;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Storm Disruptor Queue
 *
 * @author hanhan.zhang
 * */
@Slf4j
public class StormDisruptorQueue {

    private static final String PREFIX = "disruptor-";
    @Getter
    private String _queueName;

    // ring buffer for receive data
    private RingBuffer<AtomicReference<Object>> _buffer;

    // consume
    private Sequence _consume;
    private SequenceBarrier _barrier;


    // batch size
    private int _inputBatchSize;
    // key = threadId, value = thread event batch
    private final ConcurrentHashMap<Long, ThreadLocalInsert> _threadBatch = new ConcurrentHashMap<Long, ThreadLocalInsert>();


    // consume listener
    private ConsumerListener [] _listeners;

    // flush thread pool executor
    private ScheduledExecutorService _flushExecutor;

    public StormDisruptorQueue(String queueName, ProducerType type, int bufferSize, int inputBatchSize, long readTimeout, long flushInterval, ConsumerListener ... listeners) {
        this._queueName = PREFIX + queueName;
        this._inputBatchSize = inputBatchSize;

        WaitStrategy waitStrategy;
        if (readTimeout < 0) {
            waitStrategy = new LiteBlockingWaitStrategy();
        } else {
            waitStrategy = new TimeoutBlockingWaitStrategy(readTimeout, TimeUnit.MICROSECONDS);
        }
        EventFactory<AtomicReference<Object>> eventFactory = new StormEventFactory();
        this._buffer = RingBuffer.create(type, eventFactory, bufferSize, waitStrategy);

        // consume
        this._barrier = this._buffer.newBarrier();
        this._consume = new Sequence();
        this._buffer.addGatingSequences(this._consume);
        this._listeners = listeners;


        // flush thread
        this._flushExecutor = Executors.newScheduledThreadPool(1);
        this._flushExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                _threadBatch.forEach((threadId, threadLocalInsert) -> {
                    threadLocalInsert.forceBatch();
                    threadLocalInsert.flush(true);
                });
            }
        }, flushInterval, flushInterval, TimeUnit.SECONDS);
    }

    // consume
    public void consumeBatch(EventHandler<Object> eventHandler) {
        try {
            final long nextSequence = this._consume.get() + 1;
            final long availableSequence = this._barrier.waitFor(nextSequence);

            if (availableSequence > nextSequence) {
                this.consumeBatchToCursor(availableSequence, eventHandler);
            }
        } catch (AlertException e) {
            log.error("alter exception !", e);
        } catch (InterruptedException e) {
            log.error("interrupt exception !", e);
        } catch (TimeoutException e) {
            log.error("timeout exception !", e);
        }
    }

    protected void consumeBatchToCursor(long cursor, EventHandler<Object> eventHandler) {
        for (long cur = this._consume.get() + 1; cur <= cursor ; ++cur) {
            AtomicReference<Object> atomicReference = this._buffer.get(cur);
            try {
                eventHandler.onEvent(atomicReference.get(), cur, cur == cursor);
            } catch (Exception e) {
                log.error("process event exception !", e);
            } finally {
                this._consume.set(cur);
            }
        }
    }

    protected void publishDirect(ArrayList<Object> objects, boolean lock) throws InsufficientCapacityException {
        int size = objects.size();
        long endSequence;
        if (lock) {
            endSequence = this._buffer.next(size);
        } else {
            endSequence = this._buffer.tryNext(size);
        }

        long beginSequence = endSequence - (size -1);
        long index = beginSequence;
        for (Object obj : objects) {
            AtomicReference<Object> atomicReference = _buffer.get(index++);
            atomicReference.set(obj);
        }
        this._buffer.publish(beginSequence, endSequence);
        this.notifyConsumeListener();
    }

    // producer
    public void publish(Object object) {
        long threadId = Thread.currentThread().getId();
        ThreadLocalInsert threadLocalInsert = this._threadBatch.get(threadId);
        if (threadLocalInsert == null) {
            threadLocalInsert = new ThreadLocalBatch();
            this._threadBatch.put(threadId, threadLocalInsert);
        }
        threadLocalInsert.add(object);
    }

    protected void notifyConsumeListener() {
        for (ConsumerListener listener : this._listeners) {
            this.consumeBatch(listener.getConsumerHandler());
        }
    }

    protected class StormEventFactory implements EventFactory<AtomicReference<Object>> {
        @Override
        public AtomicReference<Object> newInstance() {
            return new AtomicReference<>();
        }
    }

    protected interface ThreadLocalInsert {
        public void add(Object obj);
        public void forceBatch();
        public void flush(boolean block);
    }

    // Batch for every thread
    protected class ThreadLocalBatch implements ThreadLocalInsert {

        private ReentrantLock _flushLock;

        private ArrayList<Object> _objectBatch;

        private ConcurrentLinkedQueue<ArrayList<Object>> _overflowBatch;

        public ThreadLocalBatch() {
            this._flushLock = new ReentrantLock();
            this._overflowBatch = new ConcurrentLinkedQueue<>();
            this._objectBatch = new ArrayList<>(_inputBatchSize);
        }

        @Override
        public void add(Object obj) {
            this._objectBatch.add(obj);
            // overflow
            if (this._objectBatch.size() >= _inputBatchSize) {
//                boolean flush = false;
//                if (this._overflowBatch.isEmpty()) {
//                    try {
//                        publishDirect(this._objectBatch, false);
//                        this._objectBatch.clear();
//                        flush = true;
//                    } catch (InsufficientCapacityException e) {
//
//                    }
//                }
//
//                if (!flush) {
//                    this._overflowBatch.add(this._objectBatch);
//                    this._objectBatch = new ArrayList<>(_inputBatchSize);
//                }
                this._overflowBatch.add(this._objectBatch);
                this._objectBatch = new ArrayList<>(_inputBatchSize);

            }
        }

        @Override
        public void forceBatch() {
            if (this._objectBatch.isEmpty()) {
                return;
            }
            this._overflowBatch.add(this._objectBatch);
            this._objectBatch = new ArrayList<>(_inputBatchSize);
        }

        @Override
        public void flush(boolean block) {
            if (block) {
                this._flushLock.lock();
            } else if (this._flushLock.tryLock()) {
                return;
            }

            log.info("flush thread {} event batch !", Thread.currentThread().getId());
            try {
                while (!this._overflowBatch.isEmpty()) {
                    publishDirect(this._overflowBatch.peek(), block);
                }
            } catch (InsufficientCapacityException e) {

            } finally {
                _flushLock.unlock();
            }
        }
    }
}
