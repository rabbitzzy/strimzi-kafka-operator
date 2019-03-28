/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.vertx.core.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * <p>Manages the rolling restart of a Zookeeper cluster.</p>
 *
 * <p>The following algorithm is used:</p>
 *
 * <pre>
 * For each pod:
 *   1. Test whether the pod needs to be restarted.
 *       If not then:
 *         1. Continue to the next pod
 *   2. Otherwise, check whether the pod is the leader
 *       If so, and there are still pods to be maybe-rolled then:
 *         1. Add this pod to the end of the list
 *         2. Continue to the next pod
 *   3. Otherwise:
 *       1 Restart the pod
 *       2. Wait for it to become ready (in the kube sense)
 *       3. Continue to the next pod
 * </pre>
 *
 <p>Note this algorithm still works if there is a spontaneous
 change in leader while the rolling restart is happening.</p>
 */
public class ZookeeperRoller extends Roller<Pod, ZookeeperRoller.ZkRollContext> {

    private static final Logger log = LogManager.getLogger(ZookeeperRoller.class.getName());

    public static class ZkRollContext implements Roller.Context<Pod> {

        private final List<Pod> pods;
        private final List<Pod> allPods;
        private final Secret clusterCaSecret;
        private final Secret coKeySecret;

        ZkRollContext(List<Pod> pods, Secret clusterCaSecret, Secret coKeySecret) {
            this.allPods = Collections.unmodifiableList(new ArrayList<>(pods));
            this.pods = new ArrayList<>(pods);
            this.clusterCaSecret = clusterCaSecret;
            this.coKeySecret = coKeySecret;
        }

        @Override
        public Pod next() {
            return pods.isEmpty() ? null : pods.remove(0);
        }

        public String toString() {
            return pods.stream().map(pod -> pod.getMetadata().getName()).collect(Collectors.joining(", "));
        }
    }

    private final ZookeeperLeaderFinder leaderFinder;
    private final String cluster;

    public ZookeeperRoller(long operationTimeoutMs, PodOperator podOperations, ZookeeperLeaderFinder leaderFinder, String cluster) {
        super(operationTimeoutMs, podOperations);
        this.leaderFinder = leaderFinder;
        this.cluster = cluster;
    }

    @Override
    protected Future<ZkRollContext> context(StatefulSet ss, Secret clusterCaSecret, Secret coKeySecret) {
        List<Pod> pods = new ArrayList<>();
        String cluster = ss.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL);
        // We don't really need to go getting all the pods here. ZLF doesn't need the whole pod, just it's name and a couple of other bits
        // This would avoid needing to get all the pods
        for (int i = 0; i < ss.getSpec().getReplicas(); i++) {
            Pod pod = podOperations.get(ss.getMetadata().getNamespace(), KafkaResources.zookeeperPodName(cluster, i));
            pods.add(pod);
        }
        return Future.succeededFuture(new ZkRollContext(pods, clusterCaSecret, coKeySecret));
    }

    @Override
    protected Future<ZkRollContext> sort(ZkRollContext context, Predicate<Pod> podRestart) {
        while (!context.pods.isEmpty()) {
            Pod nextPod = context.pods.get(0);
            if (!podRestart.test(nextPod)) {
                log.debug("Pod {} does not need rolling", nextPod.getMetadata().getName());
                context.pods.remove(0);
                continue;
            }
            if (context.pods.size() > 1) {
                String name = nextPod.getMetadata().getName();
                int podId = Integer.parseInt(name.substring(name.lastIndexOf('-') + 1));
                String namespace = nextPod.getMetadata().getNamespace();
                return leaderFinder.findZookeeperLeader(cluster, namespace, context.allPods, context.clusterCaSecret, context.coKeySecret)
                        .compose(leader -> {
                            if (leader == podId) {
                                context.pods.add(context.pods.remove(0));
                                return sort(context, podRestart);
                            }
                            return Future.succeededFuture(context);
                        });
            } else {
                return Future.succeededFuture(context);
            }
        }
        return Future.succeededFuture(context);
    }

    @Override
    protected Future<Void> beforeRestart(Pod pod) {
        return Future.succeededFuture();
    }

    @Override
    protected Future<Void> afterRestart(Pod pod) {
        String namespace = pod.getMetadata().getNamespace();
        String podName = pod.getMetadata().getName();
        return podOperations.readiness(namespace, podName, 1_000, operationTimeoutMs);
    }

    @Override
    protected Future<Pod> pod(StatefulSet ss, Pod pod) {
        return Future.succeededFuture(pod);
    }
}
