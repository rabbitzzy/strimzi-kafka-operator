/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.TopicConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.Integer.parseInt;

public class KafkaSorted {

    private final AdminClient ac;
    private final Future<Collection<TopicDescription>> descriptions;

    KafkaSorted(String bootstrapBroker) {
        // TODO TLS
        Properties p = new Properties();
        p.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBroker);
        p.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "");
        p.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "");
        ac = AdminClient.create(p);

        // 1. Get all topic names
        Future<Set<String>> topicNames = topicNames(ac);
        // 2. Get topic descriptions
        descriptions = topicNames.compose(names -> describeTopics(ac, names));
    }


    /**
     * Determine whether the given broker can be rolled without affecting
     * producers with acks=all publishing to topics with a {@code min.in.sync.replicas}.
     */
    Future<Boolean> canRoll(int broker) {
        return canRollBroker(descriptions, broker);
    }

    private Future<Boolean> canRollBroker(Future<Collection<TopicDescription>> descriptions, int broker) {
        Future<List<TopicDescription>> topicsOnBroker = descriptions.map(tds -> groupTopicsByBroker(tds).get(broker));

        // 4. Get topic configs (for those on $broker)
        Future<Map<String, Config>> configs = topicsOnBroker.map(td -> td.stream().map(t -> t.name()).collect(Collectors.toList()))
                .compose(names -> topicConfigs(ac, names));

        // 5. join
        return CompositeFuture.join(topicsOnBroker, configs).map(cf -> {
            Collection<TopicDescription> tds = cf.resultAt(0);
            Map<String, Config> nameToConfig = cf.resultAt(1);
            return tds.stream().anyMatch(
                td -> {
                    Config config = nameToConfig.get(td.name());
                    ConfigEntry minIsrConfig = config.get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG);
                    int minIsr;
                    if (minIsrConfig.value() != null) {
                        minIsr = parseInt(minIsrConfig.value());
                    } else {
                        minIsr = -1;
                    }
                    if (minIsr >= 0) {
                        for (TopicPartitionInfo pi : td.partitions()) {
                            if (pi.isr().size() == minIsr) {
                                for (Node b : pi.isr()) {
                                    if (b.id() == broker) {
                                        return false;
                                    }
                                }
                            }
                        }
                    }
                    return true;
                });
        });
    }


    private Future<Map<String, Config>> topicConfigs(AdminClient ac, Collection<String> topicNames) {
        List<ConfigResource> configs = topicNames.stream()
                .map((String topicName) -> new ConfigResource(ConfigResource.Type.TOPIC, topicName))
                .collect(Collectors.toList());
        Future<Map<String, Config>> f = Future.future();
        ac.describeConfigs(configs).all().whenComplete((x, error) -> {
            if (error != null) {
                f.fail(error);
            } else {
                f.complete(x.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().name(), e -> e.getValue())));
            }
        });
        return f;
    }

    private Map<Integer, List<TopicDescription>> groupTopicsByBroker(Collection<TopicDescription> tds) {
        Map<Integer, List<TopicDescription>> byBroker = new HashMap<>();
        for (TopicDescription td : tds) {
            for (TopicPartitionInfo pd : td.partitions()) {
                for (Node broker : pd.replicas()) {
                    List<TopicDescription> topicPartitionInfos = byBroker.get(broker.id());
                    if (topicPartitionInfos == null) {
                        topicPartitionInfos = new ArrayList<>();
                        byBroker.put(broker.id(), topicPartitionInfos);
                    }
                    topicPartitionInfos.add(td);
                }
            }
        }
        return byBroker;
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
}
