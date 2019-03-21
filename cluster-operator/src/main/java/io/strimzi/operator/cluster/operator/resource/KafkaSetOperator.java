/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.stream.IntStream.range;


/**
 * Specialization of {@link StatefulSetOperator} for StatefulSets of Kafka brokers
 */
public class KafkaSetOperator extends StatefulSetOperator {

    private static final Logger log = LogManager.getLogger(KafkaSetOperator.class);

    /**
     * Constructor
     *
     * @param vertx  The Vertx instance
     * @param client The Kubernetes client
     */
    public KafkaSetOperator(Vertx vertx, KubernetesClient client, long operationTimeoutMs) {
        super(vertx, client, operationTimeoutMs);
    }

    @Override
    protected boolean shouldIncrementGeneration(StatefulSet current, StatefulSet desired) {
        StatefulSetDiff diff = new StatefulSetDiff(current, desired);
        if (diff.changesVolumeClaimTemplates()) {
            log.warn("Changing Kafka storage type or size is not possible. The changes will be ignored.");
            diff = revertStorageChanges(current, desired);
        }
        return !diff.isEmpty() && needsRollingUpdate(diff);
    }

    public static boolean needsRollingUpdate(StatefulSetDiff diff) {
        if (diff.changesLabels()) {
            log.debug("Changed labels => needs rolling update");
            return true;
        }
        if (diff.changesSpecTemplate()) {
            log.debug("Changed template spec => needs rolling update");
            return true;
        }
        return false;
    }

    private void revertVolumeChanges(StatefulSet current, StatefulSet desired) {

        Container currentKafka =
                current.getSpec().getTemplate().getSpec().getContainers().stream().filter(c -> c.getName().equals("kafka")).findFirst().get();
        Container desiredKafka =
                desired.getSpec().getTemplate().getSpec().getContainers().stream().filter(c -> c.getName().equals("kafka")).findFirst().get();

        desiredKafka.setVolumeMounts(currentKafka.getVolumeMounts());

        // the external listener changed from nodeport, we need to remove rack-volume
        if (currentKafka.getEnv().stream().anyMatch(a -> a.getName().equals(KafkaCluster.ENV_VAR_KAFKA_EXTERNAL_ENABLED) && a.getValue().equals("nodeport")) &&
                desiredKafka.getEnv().stream().noneMatch(a -> a.getName().equals(KafkaCluster.ENV_VAR_KAFKA_EXTERNAL_ENABLED) && a.getValue().equals("nodeport"))) {
            desiredKafka.getVolumeMounts().remove(desiredKafka.getVolumeMounts().stream().filter(a -> a.getName().equals("rack-volume")).findFirst().get());
        }

        StatefulSet updated = new StatefulSetBuilder(desired)
                .editSpec()
                    .editTemplate()
                        .editSpec()
                            .editFirstContainer()
                                .editMatchingEnv(e -> e.getName().equals(KafkaCluster.ENV_VAR_KAFKA_LOG_DIRS))
                                    .withValue(desiredKafka.getVolumeMounts().stream()
                                        .filter(vm -> vm.getMountPath().contains(AbstractModel.VOLUME_NAME))
                                        .map(vm -> vm.getMountPath())
                                        .collect(Collectors.joining(",")))
                                .endEnv()
                            .endContainer()
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();

        desired.setSpec(updated.getSpec());
    }

    @Override
    protected StatefulSetDiff revertStorageChanges(StatefulSet current, StatefulSet desired) {

        List<PersistentVolumeClaim> currentPvcs = current.getSpec().getVolumeClaimTemplates();
        List<PersistentVolumeClaim> desiredPvcs = desired.getSpec().getVolumeClaimTemplates();

        if (desiredPvcs.size() != currentPvcs.size()) {
            log.warn("Adding or removing Kafka persistent storage is not possible. The changes will be ignored.");
            revertVolumeChanges(current, desired);
        } else {

            for (PersistentVolumeClaim currentPvc : currentPvcs) {

                Optional<PersistentVolumeClaim> pvc =
                        desiredPvcs.stream()
                                .filter(desiredPvc -> desiredPvc.getMetadata().getName().equals(currentPvc.getMetadata().getName()))
                                .findFirst();

                if (!pvc.isPresent()) {
                    log.warn("Changing Kafka persistent storage ids is not possible. The changes will be ignored.");
                    revertVolumeChanges(current, desired);

                } else if (!pvc.get().getSpec().getResources().getRequests().get("storage").getAmount()
                        .equals(currentPvc.getSpec().getResources().getRequests().get("storage").getAmount())) {

                    log.warn("Changing Kafka storage size is not possible. The changes will be ignored.");
                }
            }
        }

        return super.revertStorageChanges(current, desired);
    }

    @Override
    public Future<Void> maybeRollingUpdate(StatefulSet ss, Predicate<Pod> podRestart) {
        super.maybeRollingUpdate();

        // Get list of all pods
        // While collection not empty
        // For each pod:
        //   Does pod need restart?
        //   Is size(collection) > 1 and pod "leader":
        //     Add to end of queue
        //   Else:
        //     Precondition
        //     Restart
        //     Postcondition
        Future<List<Integer>> fut = Future.succeededFuture(range(0, ss.getSpec().getReplicas()).boxed().collect(Collectors.toList()));
        fut.compose(podIds -> {
            int size = podIds.size();
            if (size == 0) {
                return Future.succeededFuture();
            } else {
                int podId = podIds.remove(0);
                if (size == 1) {

                } else {
                    // Is it the leader?
                    return isLeader(podId).compose(isLeader -> {
                        if (isLeader) {
                            // TODO add to tail of list
                        } else {

                        }
                    });
                }
            }
        });

        return Future.succeededFuture(); // TODO
    }

    private Future<Boolean> isLeader(int podId) {
        Future<Boolean> result = Future.future();
        Properties p = new Properties();
        p.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "");
        p.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "");
        p.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "");
        AdminClient ac = AdminClient.create(p);
        ac.describeCluster().controller().whenComplete((n, e) -> {
            if (e != null) {
                result.fail(e);
            } else {
                result.complete(n.id() == podId);
            }
        });
        return result;
    }


}
