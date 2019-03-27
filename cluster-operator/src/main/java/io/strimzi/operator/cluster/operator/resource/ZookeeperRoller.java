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

public class ZookeeperRoller extends Roller<Pod, ZookeeperRoller.ZkRollContext<Pod>> {

    private static final Logger log = LogManager.getLogger(ZookeeperRoller.class.getName());

    public static class ZkRollContext<P> implements Roller.Context<P> {

        private final List<P> pods;
        private final List<P> allPods;

        ZkRollContext(List<P> pods) {
            this.allPods = Collections.unmodifiableList(new ArrayList<>(pods));
            this.pods = new ArrayList<>(pods);
        }

        @Override
        public P next() {
            return pods.remove(0);
        }

        @Override
        public boolean isEmpty() {
            return pods.isEmpty();
        }

        public String toString() {
            return pods.toString();
        }
    }

    private final ZookeeperLeaderFinder leaderFinder;
    private final Secret coKeySecret;
    private final String cluster;

    public ZookeeperRoller(long operationTimeoutMs, PodOperator podOperations, Predicate<Pod> podRestart, ZookeeperLeaderFinder leaderFinder, String cluster, Secret coKeySecret) {
        super(operationTimeoutMs, podOperations, podRestart);
        this.leaderFinder = leaderFinder;
        this.coKeySecret = coKeySecret;
        this.cluster = cluster;
    }

    @Override
    Future<ZkRollContext<Pod>> context(StatefulSet ss) {
        List<Pod> pods = new ArrayList<>();
        String cluster = ss.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL);
        // We don't really need to go getting all the pods here. ZLF doesn't need the whole pod, just it's name and a couple of other bits
        // This would avoid needing to get all the pods
        for (int i = 0; i < ss.getSpec().getReplicas(); i++) {
            Pod pod = podOperations.get(ss.getMetadata().getNamespace(), KafkaResources.zookeeperPodName(cluster, i));
            pods.add(pod);
        }
        return Future.succeededFuture(new ZkRollContext<>(pods));
    }

    @Override
    Future<ZkRollContext<Pod>> sort(ZkRollContext<Pod> context) {
        if (context.pods.size() <= 1) {
            return Future.succeededFuture(context);
        } else {
            Pod nextPod = context.pods.get(0);
            String name = nextPod.getMetadata().getName();
            int podId = Integer.parseInt(name.substring(name.lastIndexOf('-') + 1));
            String namespace = nextPod.getMetadata().getNamespace();
            return leaderFinder.findZookeeperLeader(cluster, namespace, context.allPods, coKeySecret)
                    .map(leader -> {
                        if (leader == podId) {
                            context.pods.add(context.pods.remove(0));
                        }
                        return context;
                    });
        }
    }

    @Override
    Future<Void> beforeRestart(Pod pod) {
        return Future.succeededFuture();
    }

    @Override
    Future<Void> afterRestart(Pod pod) {
        String namespace = pod.getMetadata().getNamespace();
        String podName = pod.getMetadata().getName();
        return podOperations.readiness(namespace, podName, 1_000, operationTimeoutMs);
    }

    @Override
    String podName(StatefulSet ss, Pod pod) {
        return pod.getMetadata().getName();
    }
}
