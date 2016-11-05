package com.sdu.stream.scheduler;

import com.sdu.stream.utils.CollectionUtil;
import com.sdu.stream.utils.Const;
import com.sdu.stream.utils.MapUtil;
import org.apache.storm.Config;
import org.apache.storm.scheduler.*;
import org.apache.storm.shade.com.google.common.collect.Lists;
import org.apache.storm.shade.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Storm Task Scheduler(storm.scheduler=DirectSchedule)
 *
 * @author hanhan.zhang
 * */
public class DirectSchedule implements IScheduler {

    private static final Logger LOGGER = LoggerFactory.getLogger(DirectSchedule.class);

    private boolean _debug;

    private boolean _useDefault;

    @Override
    public void prepare(Map conf) {
        this._debug = (boolean) conf.get(Config.TOPOLOGY_DEBUG);
        this._useDefault = (boolean) conf.getOrDefault(Const.STORM_USE_DEFAULT_SCHEDULE, false);
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        if (this._useDefault) {
            LOGGER.info("start to use default storm topology schedule !");
            new EvenScheduler().schedule(topologies, cluster);
        } else {
            LOGGER.info("start to use storm topology direct schedule !");
            Collection<TopologyDetails> topologyDetails = topologies.getTopologies();
            if (CollectionUtil.isNotEmpty(topologyDetails)) {
                topologyDetails.forEach(topologyDetail -> this.scheduleTopology(cluster, topologyDetail));
            }
        }
    }

    protected void scheduleTopology(Cluster cluster, TopologyDetails topologyDetail) {
        boolean needsScheduling = cluster.needsScheduling(topologyDetail);
        if (needsScheduling) {
            LOGGER.info("start to allocate resource for topology '{}' !", topologyDetail.getName());
            Map<String, List<ExecutorDetails>> componentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(topologyDetail);
            this.scheduleComponent(cluster, topologyDetail, componentToExecutors);
        } else {
            String topologyName = topologyDetail.getName();
            SchedulerAssignment schedulerAssignment = cluster.getAssignmentById(topologyDetail.getId());
            if(schedulerAssignment != null && this._debug) {
                Map<ExecutorDetails, WorkerSlot> executorToWorkerSlots = schedulerAssignment.getExecutorToSlot();
                if (MapUtil.isNotEmpty(executorToWorkerSlots)) {
                    StringBuffer sb = new StringBuffer();
                    boolean first = true;
                    for (Map.Entry<ExecutorDetails, WorkerSlot> entry : executorToWorkerSlots.entrySet()) {
                        ExecutorDetails executorDetail = entry.getKey();
                        WorkerSlot workSlot = entry.getValue();
                        if (first) {
                            sb.append(executorDetail.toString()).append("-->").append("[").append(workSlot.toString()).append("]");
                            first = false;
                        } else {
                            sb.append(",").append(executorDetail.toString()).append("-->").append("[").append(workSlot.toString()).append("]");
                        }
                    }
                    LOGGER.debug("topology '{}' already schedule, schedule message : {} .", topologyName, sb.toString());
                }
            }

            LOGGER.info("topology '{}' already schedule and don't schedule again !", topologyName);
        }
    }

    protected void scheduleComponent(Cluster cluster, TopologyDetails topologyDetail, Map<String, List<ExecutorDetails>> componentToExecutorDetails) {
        Collection<SupervisorDetails> supervisors = cluster.getSupervisors().values();

        // assign worker
        int desiredNumWorkers = topologyDetail.getNumWorkers();
        int assignNumWorkers = cluster.getAssignedNumWorkers(topologyDetail);
        int actualNeedNumWorkers = desiredNumWorkers - assignNumWorkers;
        List<WorkerSlot> assignWorkerSlots = Lists.newArrayListWithCapacity(actualNeedNumWorkers);
        supervisors.forEach(supervisorDetail -> {
            if (assignWorkerSlots.size() == actualNeedNumWorkers) {
                return;
            }
            Set<Integer> availableSlots = cluster.getAvailablePorts(supervisorDetail);
            if (CollectionUtil.isNotEmpty(availableSlots)) {
                availableSlots.forEach(availablePort -> {
                    if (assignWorkerSlots.size() == actualNeedNumWorkers) {
                        return;
                    }
                    assignWorkerSlots.add(new WorkerSlot(supervisorDetail.getId(), availablePort));
                });
            }
        });

        // assign thread(executor) for worker
        int parallelism = topologyDetail.getExecutors().size() / actualNeedNumWorkers;
        Map<WorkerSlot, Map<String, List<ExecutorDetails>>> executorToWorker = assignExecutorToWorker(parallelism, assignWorkerSlots, componentToExecutorDetails);
        executorToWorker.forEach(((workerSlot, componentToExecutors) -> {
            if (this._debug) {
                this.printScheduleMessage(topologyDetail.getName(), cluster, workerSlot, componentToExecutors);
            }

            List<ExecutorDetails> executors = Lists.newLinkedList();
            componentToExecutors.forEach((component, executorDetails) -> executors.addAll(executorDetails));

            cluster.assign(workerSlot, topologyDetail.getId(), executors);
        }));
    }

    protected Map<WorkerSlot, Map<String, List<ExecutorDetails>>> assignExecutorToWorker(int parallelism, List<WorkerSlot> workerSlots, Map<String, List<ExecutorDetails>> componentToExecutorDetails) {
        Map<WorkerSlot, Map<String, List<ExecutorDetails>>> executorToWorker = Maps.newHashMapWithExpectedSize(workerSlots.size());

        Map<String, List<ExecutorDetails>> remainMap = Maps.newHashMap();
        for (WorkerSlot workerSlot : workerSlots) {
            Map<String, List<ExecutorDetails>> result = Maps.newHashMap();
            if (MapUtil.isNotEmpty(remainMap)) {
                result.putAll(remainMap);
            }
            int remainSize = parallelism - result.size();
            if (remainSize <= 0) {
                executorToWorker.put(workerSlot, result);
                break;
            }

            Iterator<Map.Entry<String, List<ExecutorDetails>>> it = componentToExecutorDetails.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, List<ExecutorDetails>> entry = it.next();
                String component = entry.getKey();
                List<ExecutorDetails> executorDetails = entry.getValue();
                if (executorDetails.size() > remainSize) {
                    remainMap.put(component, executorDetails.subList(remainSize, executorDetails.size()));
                    result.put(component, executorDetails.subList(0, remainSize));
                    executorToWorker.put(workerSlot, result);
                    it.remove();
                    break;
                } else {
                    remainSize = remainSize - executorDetails.size();
                    result.put(component, executorDetails);
                    it.remove();
                }
            }
            executorToWorker.put(workerSlot, result);
        }
        return executorToWorker;
    }

    protected void printScheduleMessage(String topology, Cluster cluster, WorkerSlot workerSlot, Map<String, List<ExecutorDetails>> componentToExecutors) {
        SupervisorDetails supervisorDetails = cluster.getSupervisorById(workerSlot.getNodeId());
        Map meta = (Map) supervisorDetails.getMeta();
        String supervisorNode = supervisorDetails.getHost();
        String supervisorName = (String) meta.get("name");
        int port = workerSlot.getPort();
        String supervisor = new StringBuffer().append("[name:").append(supervisorName).append(",address:").append(supervisorNode).append(":").append(port).append("]").toString();
        componentToExecutors.forEach((component, executorDetails) -> {
            LOGGER.debug("executor '{}' of component '{}' of topology '{}' schedule to supervisor : {}",
                    executorDetails, component, topology, supervisor);
        });
    }
}
