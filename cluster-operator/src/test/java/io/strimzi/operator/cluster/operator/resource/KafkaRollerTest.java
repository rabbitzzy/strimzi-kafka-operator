/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.TimeoutException;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@RunWith(VertxUnitRunner.class)
public class KafkaRollerTest {

    private final Vertx vertx = Vertx.vertx();
    private List<String> restarted;

    @Before
    public void x() {
        restarted = new ArrayList<>();
    }

    @Test
    public void leaderless(TestContext testContext) {
        PodOperator podOps = mockPodOps(Future.succeededFuture());
        StatefulSet ss = buildStatefulSet();
        KafkaRollerWithControllers kafkaRoller = rollerWithControllers(podOps, -1);
        doRollingRestart(testContext, ss, kafkaRoller,
                "[c-kafka-0, c-kafka-1, c-kafka-2, c-kafka-3, c-kafka-4]");
    }

    @Test
    public void pod2IsLeader(TestContext testContext) {
        PodOperator podOps = mockPodOps(Future.succeededFuture());
        StatefulSet ss = buildStatefulSet();
        KafkaRollerWithControllers kafkaRoller = rollerWithControllers(podOps, 2);
        doRollingRestart(testContext, ss, kafkaRoller,
                "[c-kafka-0, c-kafka-1, c-kafka-3, c-kafka-4, c-kafka-2]");
    }

    @Test
    public void leaderChangesDuringRoll(TestContext testContext) {
        PodOperator podOps = mockPodOps(Future.succeededFuture());
        StatefulSet ss = buildStatefulSet();
        KafkaRollerWithControllers kafkaRoller = rollerWithControllers(podOps, 0, 1);
        doRollingRestart(testContext, ss, kafkaRoller,
                "[c-kafka-2, c-kafka-3, c-kafka-4, c-kafka-0, c-kafka-1]");
    }

    @Test
    public void podNotReadyAfterRolling(TestContext testContext) {
        PodOperator podOps = mockPodOps(Future.failedFuture(new TimeoutException("Timeout")));
        StatefulSet ss = buildStatefulSet();
        KafkaRollerWithControllers kafkaRoller = rollerWithControllers(podOps, 1);
        // TODO What should the roller do here?
        // What does/did the ZK algo do?
        doRollingRestart(testContext, ss, kafkaRoller,
                "[c-kafka-2, c-kafka-3, c-kafka-4, c-kafka-0, c-kafka-1]");
    }

    @Test
    public void errorWhenOpeningAdminClient(TestContext testContext) {
        PodOperator podOps = mockPodOps(Future.succeededFuture(null));
        StatefulSet ss = buildStatefulSet();
        KafkaRollerWithControllers kafkaRoller = new KafkaRollerWithControllers(podOps,
            new RuntimeException("Test Exception"),
            null,
            brokerId -> Future.succeededFuture(true),
            2);
        // The algorithm should carry on rolling the pods (errors are logged),
        // because we never find the controller we get ascending order
        doRollingRestart(testContext, ss, kafkaRoller,
                "[c-kafka-0, c-kafka-1, c-kafka-2, c-kafka-3, c-kafka-4]");
    }

    @Test
    public void errorWhenClosingAdminClient(TestContext testContext) {
        PodOperator podOps = mockPodOps(Future.succeededFuture());
        StatefulSet ss = buildStatefulSet();
        KafkaRollerWithControllers kafkaRoller = new KafkaRollerWithControllers(podOps,
            null,
            new RuntimeException("Test Exception"),
            brokerId -> Future.succeededFuture(true),
            2);
        // The algorithm should carry on rolling the pods (errors are logged),
        // because we did the controller we controller last order
        doRollingRestart(testContext, ss, kafkaRoller,
                "[c-kafka-0, c-kafka-1, c-kafka-3, c-kafka-4, c-kafka-2]");
    }

    @Test
    public void nonControllerNotInitiallyRollable(TestContext testContext) {
        PodOperator podOps = mockPodOps(Future.succeededFuture());
        StatefulSet ss = buildStatefulSet();
        AtomicInteger count = new AtomicInteger(3);
        KafkaRollerWithControllers kafkaRoller = new KafkaRollerWithControllers(podOps,
            null, null,
            brokerId ->
                    brokerId == 1 ? Future.succeededFuture(count.getAndDecrement() == 0 ? true : false)
                            : Future.succeededFuture(true),
            2);
        doRollingRestart(testContext, ss, kafkaRoller,
                "[c-kafka-0, c-kafka-3, c-kafka-4, c-kafka-1, c-kafka-2]");
    }

    @Test
    public void controllerNotInitiallyRollable(TestContext testContext) {
        PodOperator podOps = mockPodOps(Future.succeededFuture());
        StatefulSet ss = buildStatefulSet();
        AtomicInteger count = new AtomicInteger(3);
        KafkaRollerWithControllers kafkaRoller = new KafkaRollerWithControllers(podOps,
            null, null,
            brokerId ->
                    brokerId == 2 ? Future.succeededFuture(count.getAndDecrement() == 0 ? true : false)
                            : Future.succeededFuture(true),
            2);
        doRollingRestart(testContext, ss, kafkaRoller,
                "[c-kafka-0, c-kafka-1, c-kafka-3, c-kafka-4, c-kafka-2]");
    }

    @Test
    public void nonControllerNeverRollable(TestContext testContext) {
        PodOperator podOps = mockPodOps(Future.succeededFuture());
        StatefulSet ss = buildStatefulSet();
        KafkaRollerWithControllers kafkaRoller = new KafkaRollerWithControllers(podOps,
            null, null,
            brokerId ->
                    brokerId == 1 ? Future.succeededFuture(false)
                            : Future.succeededFuture(true),
            2);
        doRollingRestart(testContext, ss, kafkaRoller,
                "[c-kafka-0, c-kafka-3, c-kafka-4, c-kafka-1, c-kafka-2]");
    }

    @Test
    public void controllerNeverRollable(TestContext testContext) {
        PodOperator podOps = mockPodOps(Future.succeededFuture());
        StatefulSet ss = buildStatefulSet();
        KafkaRollerWithControllers kafkaRoller = new KafkaRollerWithControllers(podOps,
            null, null,
            brokerId ->
                    brokerId == 2 ? Future.succeededFuture(false)
                            : Future.succeededFuture(true),
            2);
        doRollingRestart(testContext, ss, kafkaRoller,
                "[c-kafka-0, c-kafka-1, c-kafka-3, c-kafka-4, c-kafka-2]");
    }

    KafkaRollerWithControllers rollerWithControllers(PodOperator podOps, int... controllers) {
        return new KafkaRollerWithControllers(podOps,
            null, null,
            brokerId -> Future.succeededFuture(true),
            controllers);
    }

    void doRollingRestart(TestContext testContext, StatefulSet ss, KafkaRollerWithControllers kafkaRoller, String expected) {
        Async async = testContext.async();
        kafkaRoller.rollingRestart(ss, null, null, pod -> true).setHandler(ar -> {
            if (ar.failed()) {
                testContext.fail(new RuntimeException("Rolling failed", ar.cause()));
            }
            testContext.assertEquals(expected, restarted.toString());
            if (!kafkaRoller.unclosedAdminClients.isEmpty()) {
                Throwable alloc = kafkaRoller.unclosedAdminClients.values().iterator().next();
                alloc.printStackTrace(System.out);
                testContext.fail(kafkaRoller.unclosedAdminClients.size() + " unclosed AdminClient instances");
            }
            async.complete();
        });
    }

    StatefulSet buildStatefulSet() {
        return new StatefulSetBuilder()
                    .withNewMetadata()
                        .withName("c-kafka")
                        .withNamespace("ns")
                        .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, "c")
                    .endMetadata()
                    .withNewSpec()
                        .withReplicas(5)
                    .endSpec()
                .build();
    }

    PodOperator mockPodOps(Future<Void> readiness) {
        PodOperator podOps = mock(PodOperator.class);
        when(podOps.getAsync(any(), any())).thenAnswer(
            invocation -> Future.succeededFuture(new PodBuilder()
                    .withNewMetadata()
                        .withNamespace(invocation.getArgument(0))
                        .withName(invocation.getArgument(1))
                    .endMetadata()
                .build())
        );
        when(podOps.readiness(any(), any(), anyLong(), anyLong())).thenReturn(readiness);
        return podOps;
    }

    private class KafkaRollerWithControllers extends KafkaRoller {
        private final int[] controllers;
        private final Throwable acOpenException;
        private final Throwable acCloseException;
        private final Function<Integer, Future<Boolean>> canRollFn;
        private IdentityHashMap<AdminClient, Throwable> unclosedAdminClients = new IdentityHashMap<>();

        public KafkaRollerWithControllers(PodOperator podOps, Throwable acOpenException, Throwable acCloseException,
                                          Function<Integer, Future<Boolean>> canRollFn,
                                          int... controllers) {
            super(KafkaRollerTest.this.vertx, podOps, 500, 1000);
            this.controllers = controllers;
            this.controllerCall = 0;
            this.acOpenException = acOpenException;
            this.acCloseException = acCloseException;
            this.canRollFn = canRollFn;
        }

        @Override
        protected Future<AdminClient> adminClient(KafkaRollContext context, Pod pod) {
            if (acOpenException != null) {
                return Future.failedFuture(acOpenException);
            }
            context.ac = mock(AdminClient.class, invocation -> {
                if ("close".equals(invocation.getMethod().getName())) {
                    unclosedAdminClients.remove(invocation.getMock());
                    if (acCloseException != null) {
                        throw acCloseException;
                    }
                    return null;
                }
                throw new RuntimeException("Not mocked " + invocation.getMethod());
            });
            unclosedAdminClients.put(context.ac, new Throwable("Pod " + pod.getMetadata().getName()));
            return Future.succeededFuture(context.ac);
        }

        @Override
        protected KafkaSorted getKs(KafkaRollContext context) {
            return new KafkaSorted(null) {
                @Override
                protected Future<Set<String>> topicNames() {
                    return Future.succeededFuture(Collections.emptySet());
                }

                @Override
                protected Future<Collection<TopicDescription>> describeTopics(Set<String> names) {
                    return Future.succeededFuture(Collections.emptySet());
                }

                @Override
                Future<Boolean> canRoll(int broker) {
                    return canRollFn.apply(broker); // Future.succeededFuture(true);
                }
            };
        }

//        @Override
//        protected void close(KafkaRollContext context, Future<KafkaRollContext> result, AsyncResult<KafkaRollContext> ar) {
//            unclosedAdminClients.remove(context.ac);
//            if (ar.failed()) {
//                result.fail(ar.cause());
//            } else {
//                result.complete(context);
//            }
//        }

        @Override
        protected Future<Void> restart(StatefulSet ss, Pod pod) {
            restarted.add(pod.getMetadata().getName());
            return Future.succeededFuture();
        }

        int controllerCall;

        @Override
        Future<Integer> controller(AdminClient ac) {
            int index;
            if (controllerCall < controllers.length) {
                index = controllerCall;
            } else {
                index = controllers.length - 1;
            }
            controllerCall++;
            return Future.succeededFuture(controllers[index]);
        }
    }

    // TODO Different scenarios around controller, and unrollable brokers
    //   TODO A non-controller broker remains unrollable
    //   TODO A controller broker remains unrollable
    //   TODO Error when finding the next broker
}
