/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class KafkaSortedTest {


    class KSB {
        class TSB {
            class PSB {
                private final Integer id;
                private int[] isr;
                private int leader;
                private int[] replicaOn;

                public PSB(Integer p) {
                    this.id = p;
                }
                PSB replicaOn(int... broker) {
                    broker(broker);
                    this.replicaOn = broker;
                    return this;
                }

                PSB leader(int broker) {
                    broker(broker);
                    this.leader = broker;
                    return this;
                }

                PSB isr(int... broker) {
                    broker(broker);
                    this.isr = broker;
                    return this;
                }
                TSB endPartition() {
                    if (!asList(this.replicaOn).contains(this.leader)) {
                        throw new RuntimeException("Leader must be one of the replicas");
                    }
                    if (!asList(this.replicaOn).containsAll(asList(this.isr))) {
                        throw new RuntimeException("ISR must be a subset of the replicas");
                    }
                    if (asList(this.isr).contains(this.leader)) {
                        throw new RuntimeException("ISR must not include the leader");
                    }
                    return TSB.this;
                }
            }
            private final String name;
            private final boolean internal;
            private Map<String, String> configs = new HashMap<>();
            private Map<Integer, PSB> partitions = new HashMap<>();

            public TSB(String name, boolean internal) {
                this.name = name;
                this.internal = internal;
                KSB.this.topics.put(name, this);
            }

            TSB addToConfig(String config, String value) {
                configs.put(config, value);
                return this;
            }
            PSB partition(int partition) {
                return partitions.computeIfAbsent(partition, p -> new PSB(p));
            }


            KSB endTopic() {
                return KSB.this;
            }
        }

        class BSB {

            private int id;

            public BSB(int id) {
                this.id = id;
                KSB.this.brokers.put(id, this);
                KSB.this.nodes.put(id, new Node(id, "localhost", 1234 + id));
            }

            KSB endBroker() {
                return KSB.this;
            }
        }

        private Map<String, TSB> topics = new HashMap<>();
        private Map<Integer, BSB> brokers = new HashMap<>();
        private Map<Integer, Node> nodes = new HashMap<>();

        TSB topic(String name, boolean internal) {
            return topics.computeIfAbsent(name, n -> new TSB(n, internal));
        }

        void broker(int... ids) {
            for (int id : ids) {
                brokers.computeIfAbsent(id, i -> new BSB(i));
            }
        }

        ListTopicsResult ltr() {
            ListTopicsResult ltr = mock(ListTopicsResult.class);
            when(ltr.names()).thenReturn(KafkaFuture.completedFuture(new HashSet<>(topics.keySet())));
            return ltr;
        }

        DescribeTopicsResult dtr() {
            Map<String, TopicDescription> tds = topics.entrySet().stream().collect(Collectors.toMap(
                e -> e.getKey(),
                e -> {
                    TSB tsb = e.getValue();
                    return new TopicDescription(tsb.name, tsb.internal,
                            tsb.partitions.entrySet().stream().map(e1 -> {
                                TSB.PSB psb = e1.getValue();
                                return new TopicPartitionInfo(psb.id,
                                        node(psb.leader),
                                        Arrays.stream(psb.replicaOn).boxed().map(broker -> node(broker)).collect(Collectors.toList()),
                                        Arrays.stream(psb.isr).boxed().map(broker -> node(broker)).collect(Collectors.toList()));
                            }).collect(Collectors.toList()));
                }
            ));
            DescribeTopicsResult dtr = mock(DescribeTopicsResult.class);
            when(dtr.all()).thenReturn(KafkaFuture.completedFuture(tds));
            return dtr;
        }

        private Node node(int id) {
            return nodes.computeIfAbsent(id, x -> {
                throw new RuntimeException("Unknown node " + id);
            });
        }

        DescribeConfigsResult dcr(Collection<ConfigResource> argument) {
            Map<ConfigResource, Config> result = new HashMap<>();
            for (ConfigResource cr : argument) {
                List<ConfigEntry> entries = new ArrayList<>();
                for (Map.Entry<String, String> e : topics.get(cr.name()).configs.entrySet()) {
                    ConfigEntry ce = new ConfigEntry(e.getKey(), e.getValue());
                    entries.add(ce);
                }
                result.put(cr, new Config(entries));
            }
            DescribeConfigsResult dcr = mock(DescribeConfigsResult.class);
            when(dcr.all()).thenReturn(KafkaFuture.completedFuture(result));
            return dcr;
        }

        AdminClient ac() {
            AdminClient ac = mock(AdminClient.class);

            ListTopicsResult ltr = ltr();
            when(ac.listTopics(any())).thenReturn(ltr);

            DescribeTopicsResult dtr = dtr();
            when(ac.describeTopics(any())).thenReturn(dtr);

            when(ac.describeConfigs(any())).thenAnswer(invocation -> dcr(invocation.getArgument(0)));
            return ac;
        }
    }

    @Test
    public void test(TestContext context) {
        // A minisr = 2
        // B minisr = 2
        // Broker 0, leader: A/0, isr: B/0, CONTROLLER
        // Broker 1, leader: B/0, isr: A/0
        KSB ksb = new KSB().topic("A", false)
                .addToConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
                .partition(0)
                    .replicaOn(0, 1)
                    .leader(0)
                    .isr(0, 1)
                .endPartition()

                .endTopic()
                .topic("B", false)
                .addToConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
                .partition(0)
                    .replicaOn(0, 1)
                    .leader(1)
                    .isr(0)
                .endPartition()

                .endTopic();

        KafkaSorted kafkaSorted = new KafkaSorted(ksb.ac());

        for (int i = 0; i <= 1; i++) {
            int brokerId = i;
            Async async = context.async();

            kafkaSorted.canRoll(brokerId).setHandler(ar -> {
                if (ar.failed()) {
                    context.fail(ar.cause());
                } else {
                    context.assertFalse(ar.result(),
                            "broker " + brokerId + " should not be rollable, being minisr = 2 and it's only replicated on two brokers");
                }
                async.complete();
            });
        }
    }
/*
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties p = new Properties();
        p.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient ac = AdminClient.create(p);
        String topic1 = "topic2";
        ac.createTopics(asList(new NewTopic(topic1, 1, (short) 2)));
        TopicDescription topicDescription = ac.describeTopics(asList(topic1)).all().get().get(topic1);
        System.out.println(topicDescription.partitions().get(0).replicas());
        System.out.println(topicDescription.partitions().get(0).isr());
        System.out.println(topicDescription.partitions().get(0).leader());
    }
*/
}
