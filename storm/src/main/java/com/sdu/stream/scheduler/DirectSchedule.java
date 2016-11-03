package com.sdu.stream.scheduler;

import com.sdu.stream.utils.Const;
import org.apache.storm.Config;
import org.apache.storm.scheduler.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Storm Task Scheduler(storm.scheduler=DirectSchedule)
 *
 * @author hanhan.zhang
 * */
public class DirectSchedule implements IScheduler {

    private static final Logger LOGGER = LoggerFactory.getLogger(DirectSchedule.class);

    private boolean _debug;

    private boolean _useDefault;

    private boolean _occupy;

    private int _occupyPort;

    @Override
    public void prepare(Map conf) {
        this._debug = (boolean) conf.get(Config.TOPOLOGY_DEBUG);
        this._useDefault = (boolean) conf.get(Const.STORM_USE_DEFALUT_SCHEDULE);
        this._occupy = (boolean) conf.get(Const.SUPERVISOR_SLOT_OCCUPY);
        this._occupyPort = (int) conf.get(Const.SUPERVISOR_SLOT_OCCUPY_PORT);
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        if (this._useDefault) {
            LOGGER.info("start to use default storm topology schedule !");
            new EvenScheduler().schedule(topologies, cluster);
        } else {
            LOGGER.info("start to use storm topology direct schedule !");
            Collection<TopologyDetails> topologyDetails = topologies.getTopologies();
            topologyDetails.forEach(topologyDetail -> this.scheduleTopology(cluster, topologyDetail));
        }
    }

    protected void scheduleTopology(Cluster cluster, TopologyDetails topologyDetail) {
        boolean needsScheduling = cluster.needsScheduling(topologyDetail);
        if (needsScheduling) {
            LOGGER.info("start to allocate resource for topology '{}' !", topologyDetail.getName());
            Map<String, List<ExecutorDetails>> componentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(topologyDetail);

            if (this._debug) {
                componentToExecutors.forEach((componentId, executorDetails) -> {
                    String msg = this.collectionToString(executorDetails, ",");
                    LOGGER.debug("component '{}' need to allocate executor : {} .", componentId, msg);
                });
            }

            SchedulerAssignment schedulerAssignment = cluster.getAssignmentById(topologyDetail.getId());
            if (schedulerAssignment != null) {
                if (this._debug) {
                    Map<ExecutorDetails, WorkerSlot> executorToWorkerSlots = schedulerAssignment.getExecutorToSlot();
                    if (executorToWorkerSlots != null && !executorToWorkerSlots.isEmpty()) {
                        executorToWorkerSlots.forEach((executorDetail, workerSlot) ->
                                LOGGER.debug("executor '{}' allocate to worker '{}:{}' .", executorDetail,
                                        workerSlot.getNodeId(), workerSlot.getPort())
                        );
                    }
                }
            } else {
                // schedule component
                this.scheduleComponent(cluster, topologyDetail, componentToExecutors);
            }

        } else {
            String topologyName = topologyDetail.getName();
            LOGGER.info("topology '{}' already schedule and don't schedule again !", topologyName);
        }
    }

    protected void scheduleComponent(Cluster cluster, TopologyDetails topologyDetail, Map<String, List<ExecutorDetails>> componentToExecutorDetails) {
        Collection<SupervisorDetails> supervisors = cluster.getSupervisors().values();

        componentToExecutorDetails.forEach((component, executorDetail) -> {
            LOGGER.info("start allocate executor for component '{}' !", component);

            supervisors.forEach(supervisorDetail -> {
                Map meta = (Map) supervisorDetail.getSchedulerMeta();
                /**
                 * supervisor.scheduler.meta :
                 * name : 'supervisorName'
                 * */
                String supervisorName = (String) meta.get("name");
                String supervisorId = supervisorDetail.getId();
                Double memory = supervisorDetail.getTotalMemory();
                Double cpu = supervisorDetail.getTotalCPU();
                Set<Integer> assignablePorts = supervisorDetail.getAllPorts();
                List<WorkerSlot> availableSlots = cluster.getAvailableSlots(supervisorDetail);
                String assignPortMsg = collectionToString(assignablePorts, ",");
                if (availableSlots.isEmpty()) {
                    if(this._debug) {
                        LOGGER.debug("supervisor [{}-{}] allocate memory {} MB and {} CPU and occupy port {}, but now available port is zero !",
                                supervisorId, supervisorName, memory, cpu, assignPortMsg);
                    }
                    // occupy used port
                    if (this._occupy) {
                        Set<Integer> usedPorts = cluster.getUsedPorts(supervisorDetail);
                        usedPorts.forEach(usedPort -> {
                            if (usedPort == this._occupyPort) {
                                LOGGER.info("supervisor [{}-{}] free used port {} !", supervisorId, supervisorName, usedPort);
                                cluster.freeSlot(new WorkerSlot(supervisorId, usedPort));
                            }
                        });
                    }
                }

                availableSlots = cluster.getAvailableSlots(supervisorDetail);
                String availablePortMsg = collectionToString(availableSlots, ",");

                if (this._debug) {
                    LOGGER.info("supervisor [{}-{}] allocate memory {} MB and {} CPU and occupy port {}, available port : {} !",
                            supervisorId, supervisorName, memory, cpu, assignPortMsg, availablePortMsg);
                }

                cluster.assign(availableSlots.get(0), topologyDetail.getId(), executorDetail);
            });
        });
    }

    protected <T> String collectionToString(Collection<T> collection, String separator) {
        if (collection == null || collection.isEmpty()) {
            return "";
        }
        boolean first = true;
        StringBuffer sb = new StringBuffer();
        for (T item : collection) {
            if (first) {
                sb.append("{").append(item);
                first = false;
            } else {
                sb.append(separator).append(item);
            }
        }
        sb.append("}");
        return sb.toString();
    }
}
