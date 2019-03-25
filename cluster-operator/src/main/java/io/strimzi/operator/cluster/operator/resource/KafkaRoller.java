/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.strimzi.operator.cluster.operator.resource.Roller.ListContext;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.vertx.core.Future;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.stream.IntStream.range;

class KafkaRoller extends Roller<Integer, ListContext<Integer>> {

    private static final Logger log = LogManager.getLogger(KafkaRoller.class.getName());

    private final PodOperator podOperations;
    private final long pollingIntervalMs;
    private final long operationTimeoutMs;

    public KafkaRoller(PodOperator podOperations, Predicate<Pod> podRestart, long pollingIntervalMs, long operationTimeoutMs) {
        super(0, podOperations, podRestart);
        this.podOperations = podOperations;
        this.pollingIntervalMs = pollingIntervalMs;
        this.operationTimeoutMs = operationTimeoutMs;
    }

    @Override
    Future<ListContext<Integer>> context(StatefulSet ss) {
        List<Integer> collect = range(0, ss.getSpec().getReplicas()).boxed().collect(Collectors.toList());
        return Future.succeededFuture(new ListContext<>(collect));
    }

    @Override
    Future<ListContext<Integer>> sort(ListContext<Integer> context) {
        if (context.size() <= 1) {
            return Future.succeededFuture(context);
        } else {
            Integer podId = context.remainingPods().get(0);
            String hostname = "" + podId;
            return leader(hostname).compose(leader -> {
                if (podId.equals(leader)) {
                    log.debug("Deferring possible roll of pod {}", podId);
                    context.addLast(context.next());
                }
                return Future.succeededFuture(context);
            });
        }
    }

    Future<Integer> leader(String bootstrapBroker) {
        // TODO retry
        // TODO TLS
        Future<Integer> result = Future.future();
        AdminClient ac = getAdminClient(bootstrapBroker);
        ac.describeCluster().controller().whenComplete((controllerNode, exception) -> {
            if (exception != null) {
                result.fail(exception);
            } else {
                result.complete(controllerNode.id());
            }
        });
        return result;
    }

    Future<Integer> leader2(String bootstrapBroker) {
        // TODO retry
        // TODO TLS
        Future<Integer> result = Future.future();
        AdminClient ac = getAdminClient(bootstrapBroker);
        // Get all topic names
        Future<Collection<TopicDescription>> compose = topicNames(ac)
                // Get topic descriptions
                .compose(names -> describeTopics(ac, names));
        // Group topics by broker
        compose
            .map(tds -> groupReplicasByBroker(tds));
        // Get topic config for next broker
        compose
            .map(tds -> {
                List<ConfigResource> topicNames = tds.stream().map(td -> td.name())
                        .map((String topicName) -> new ConfigResource(ConfigResource.Type.TOPIC, topicName))
                        .collect(Collectors.toList());
                Future f = Future.future();
                ac.describeConfigs(topicNames).all().whenComplete((x, error) -> {
                    if (error != null) {
                        f.fail(error);
                    } else {
                        Map<ConfigResource, Config> x1 = x;
                        ConfigResource cr = null;
                        x1.get(cr).get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG);
                    }
                });
                return null;
            });

        return result;
    }

    private Map<Node, List<TopicPartitionInfo>> groupReplicasByBroker(Collection<TopicDescription> tds) {
        Map<Node, List<TopicPartitionInfo>> byBroker = new HashMap<>();
        for (TopicDescription td : tds) {
            for (TopicPartitionInfo pd : td.partitions()) {
                for (Node broker : pd.replicas()) {
                    List<TopicPartitionInfo> topicPartitionInfos = byBroker.get(broker);
                    if (topicPartitionInfos == null) {
                        topicPartitionInfos = new ArrayList<>();
                        byBroker.put(broker, topicPartitionInfos);
                    }
                    topicPartitionInfos.add(pd);
                }
            }
        }
        return byBroker;
    }

    private Future<Collection<TopicDescription>> describeTopics(AdminClient ac, Set<String> names) {
        Future<Collection<TopicDescription>> descFuture = Future.future();
        ac.describeTopics(names).all()
                .whenComplete((tds, error) -> {
                    if (error != null) {
                        descFuture.fail(error);
                    } else {
                        descFuture.complete(tds.values());
                    }
                });
        return descFuture;
    }

    private Future<Set<String>> topicNames(AdminClient ac) {
        Future<Set<String>> namesFuture = Future.future();
        ac.listTopics(new ListTopicsOptions().listInternal(true)).names()
                .whenComplete((names, error) -> {
                    if (error != null) {
                        namesFuture.fail(error);
                    } else {
                        namesFuture.complete(names);
                    }
                });
        return namesFuture;
    }

    private AdminClient getAdminClient(String bootstrapBroker) {
        Properties p = new Properties();
        p.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBroker);
        p.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "");
        p.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "");
        return AdminClient.create(p);
    }


    @Override
    Future<Void> beforeRestart(Pod pod) {
        // TODO Wait until all the partitions which the broker is replicating won't be under their ISR
        return Future.succeededFuture();
    }

    @Override
    Future<Void> afterRestart(Pod pod) {
        String namespace = pod.getMetadata().getNamespace();
        String podName = pod.getMetadata().getName();
        return podOperations.readiness(namespace, podName, pollingIntervalMs, operationTimeoutMs);
    }

    @Override
    String podName(StatefulSet ss, Integer podId) {
        return ss.getMetadata().getName() + "-" + podId;
    }

}
