/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.vertx.core.Future;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class RollerTest {

    private static final Logger log = LogManager.getLogger(RollerTest.class.getName());

    public static class ListContext<P> implements Roller.Context<P> {

        private final List<P> pods;

        ListContext(List<P> pods) {
            this.pods = new ArrayList<>(pods);
        }

        @Override
        public P next() {
            return pods.isEmpty() ? null : pods.remove(0);
        }

        public String toString() {
            return pods.toString();
        }
    }

    @Test
    public void testLeaderRolledLast(TestContext context) {
        Function<ListContext<Integer>, Future<ListContext<Integer>>> sort = listContext -> {
            if (!listContext.pods.isEmpty() && listContext.pods.get(0).equals(3)) {
                log.debug("Pod 3 must be rolled last");
                listContext.pods.add(listContext.next());
            }
            return Future.succeededFuture(listContext);
        };
        String expected = "[foo-kafka-0, foo-kafka-1, foo-kafka-2, foo-kafka-4, foo-kafka-3]";

        rollWithSort(context, sort, expected);
    }

    /**
     * Test when leadership changes during roll
     */
    @Test
    public void testLeaderChangesDuringRoll(TestContext context) {
        Function<ListContext<Integer>, Future<ListContext<Integer>>> sort = new Function<ListContext<Integer>, Future<ListContext<Integer>>>() {
            boolean changedLeader = false;

            @Override
            public Future<ListContext<Integer>> apply(ListContext<Integer> listContext) {
                int leader = !changedLeader ? 1 : 3;
                if (!listContext.pods.isEmpty() && listContext.pods.get(0).equals(leader)) {
                    log.debug("Pod {} must be rolled last", leader);
                    listContext.pods.add(listContext.next());
                    changedLeader = true;
                }
                return Future.succeededFuture(listContext);
            }
        };
        String expected = "[foo-kafka-0, foo-kafka-2, foo-kafka-4, foo-kafka-1, foo-kafka-3]";

        rollWithSort(context, sort, expected);
    }

    void rollWithSort(TestContext context, Function<ListContext<Integer>, Future<ListContext<Integer>>> sort, String expectedRollOrder) {
        PodOperator po = mock(PodOperator.class);
        when(po.getAsync(any(), any())).thenAnswer(invocation -> {
            String name = invocation.getArgument(1);
            return Future.succeededFuture(new PodBuilder()
                    .withNewMetadata()
                    .withName(name)
                    .withNamespace("ns")
                    .endMetadata()
                    .build());
        });
        List<String> rolled = new ArrayList<>();
        List<String> pre = new ArrayList<>();
        List<String> post = new ArrayList<>();

        Roller<Integer, ListContext<Integer>> roller = new Roller<Integer, ListContext<Integer>>(0, po) {

            @Override
            protected Future<ListContext<Integer>> context(StatefulSet ss, Secret s, Secret s2) {
                return Future.succeededFuture(new ListContext<>(IntStream.range(0, 5).boxed().collect(Collectors.toList())));
            }

            @Override
            protected Future<ListContext<Integer>> sort(ListContext<Integer> context, Predicate<Pod> restartPod) {
                return sort.apply(context);
            }

            @Override
            protected Future<Void> beforeRestart(Pod pod) {
                pre.add(pod.getMetadata().getName());
                return Future.succeededFuture();
            }

            @Override
            protected Future<Void> afterRestart(Pod pod) {
                post.add(pod.getMetadata().getName());
                return Future.succeededFuture();
            }

            @Override
            protected Future<Pod> pod(StatefulSet ss, Integer pod) {
                return Future.succeededFuture(new PodBuilder()
                    .withNewMetadata()
                        .withNamespace(ss.getMetadata().getNamespace())
                        .withName(ss.getMetadata().getName() + "-" + pod)
                    .endMetadata()
                    .build());
            }

            @Override
            protected Future<Void> restart(StatefulSet ss, Pod pod) {
                rolled.add(pod.getMetadata().getName());
                return Future.succeededFuture();
            }
        };
        Async async = context.async();
        StatefulSet ss = new StatefulSetBuilder()
                .withNewMetadata()
                .withName("foo-kafka")
                .withNamespace("ns")
                .endMetadata()
                .withNewSpec()
                .withReplicas(5)
                .endSpec()
                .build();
        roller.rollingRestart(ss, null, null, pod -> true).setHandler(ar -> {
            if (ar.failed()) {
                context.fail(ar.cause());
            } else {
                context.assertEquals(expectedRollOrder, rolled.toString());
                context.assertEquals(expectedRollOrder, pre.toString());
                context.assertEquals(expectedRollOrder, post.toString());
            }
            async.complete();
        });
    }

    @Test
    @Ignore
    public void testRollingThrows(TestContext context) {
        PodOperator po = mock(PodOperator.class);
        when(po.getAsync(any(), any())).thenAnswer(invocation -> {
            String name = invocation.getArgument(1);
            return Future.succeededFuture(new PodBuilder()
                    .withNewMetadata()
                    .withName(name)
                    .withNamespace("ns")
                    .endMetadata()
                    .build());
        });
        List<String> rolled = new ArrayList<>();
        List<String> pre = new ArrayList<>();
        List<String> post = new ArrayList<>();

        Roller<Integer, ListContext<Integer>> roller = new Roller<Integer, ListContext<Integer>>(0, po) {

            @Override
            protected Future<ListContext<Integer>> context(StatefulSet ss, Secret s, Secret s2) {
                return Future.succeededFuture(new ListContext<>(IntStream.range(0, 5).boxed().collect(Collectors.toList())));
            }

            @Override
            protected Future<ListContext<Integer>> sort(ListContext<Integer> context, Predicate<Pod> restartPod) {
                return Future.succeededFuture(context);
            }

            @Override
            protected Future<Void> beforeRestart(Pod pod) {
                pre.add(pod.getMetadata().getName());
                return Future.succeededFuture();
            }

            @Override
            protected Future<Void> afterRestart(Pod pod) {
                post.add(pod.getMetadata().getName());
                return Future.succeededFuture();
            }

            @Override
            protected Future<Void> restart(StatefulSet ss, Pod pod) {
                rolled.add(pod.getMetadata().getName());
                if (pod.getMetadata().getName().equals("foo-kafka-2")) {
                    return Future.failedFuture(new RuntimeException("Test exception"));
                } else {
                    return Future.succeededFuture();
                }
            }

            @Override
            protected Future<Pod> pod(StatefulSet ss, Integer pod) {
                return Future.succeededFuture(new PodBuilder()
                        .withNewMetadata()
                        .withNamespace(ss.getMetadata().getNamespace())
                        .withName(ss.getMetadata().getName() + "-" + pod)
                        .endMetadata()
                        .build());
            }
        };
        Async async = context.async();
        StatefulSet ss = new StatefulSetBuilder()
                .withNewMetadata()
                .withName("foo-kafka")
                .withNamespace("ns")
                .endMetadata()
                .withNewSpec()
                .withReplicas(5)
                .endSpec()
                .build();
        roller.rollingRestart(ss, null, null, pod -> true).setHandler(ar -> {
            if (ar.failed()) {
                context.fail(ar.cause());
            } else {
                context.assertEquals("", rolled.toString());
                context.assertEquals("", pre.toString());
                context.assertEquals("", post.toString());
            }
            async.complete();
        });
    }

    @Test
    public void testSortingThrows(TestContext context) {
        PodOperator po = mock(PodOperator.class);
        when(po.getAsync(any(), any())).thenAnswer(invocation -> {
            String name = invocation.getArgument(1);
            return Future.succeededFuture(new PodBuilder()
                    .withNewMetadata()
                    .withName(name)
                    .withNamespace("ns")
                    .endMetadata()
                    .build());
        });
        List<String> rolled = new ArrayList<>();
        List<String> pre = new ArrayList<>();
        List<String> post = new ArrayList<>();

        Roller<Integer, ListContext<Integer>> roller = new Roller<Integer, ListContext<Integer>>(0, po) {

            int call = 0;

            @Override
            protected Future<ListContext<Integer>> context(StatefulSet ss, Secret s, Secret s2) {
                return Future.succeededFuture(new ListContext(IntStream.range(0, 5).boxed().collect(Collectors.toList())));
            }

            @Override
            protected Future<ListContext<Integer>> sort(ListContext<Integer> context, Predicate<Pod> restartPod) {
                if (call++ == 3) {
                    return Future.failedFuture(new RuntimeException("Test exception"));
                }
                return Future.succeededFuture(context);
            }

            @Override
            protected Future<Void> beforeRestart(Pod pod) {
                pre.add(pod.getMetadata().getName());
                return Future.succeededFuture();
            }

            @Override
            protected Future<Void> afterRestart(Pod pod) {
                post.add(pod.getMetadata().getName());
                return Future.succeededFuture();
            }

            @Override
            protected Future<Void> restart(StatefulSet ss, Pod pod) {
                rolled.add(pod.getMetadata().getName());
                return Future.succeededFuture();
            }

            @Override
            protected Future<Pod> pod(StatefulSet ss, Integer pod) {
                return Future.succeededFuture(new PodBuilder()
                        .withNewMetadata()
                        .withNamespace(ss.getMetadata().getNamespace())
                        .withName(ss.getMetadata().getName() + "-" + pod)
                        .endMetadata()
                        .build());
            }
        };
        Async async = context.async();
        StatefulSet ss = new StatefulSetBuilder()
                .withNewMetadata()
                .withName("foo-kafka")
                .withNamespace("ns")
                .endMetadata()
                .withNewSpec()
                .withReplicas(5)
                .endSpec()
                .build();
        roller.rollingRestart(ss, null, null, pod -> true).setHandler(ar -> {
            if (ar.failed()) {
                context.fail(ar.cause());
            } else {
                context.assertEquals("[foo-kafka-0, foo-kafka-1, foo-kafka-2, foo-kafka-3, foo-kafka-4]", rolled.toString());
                context.assertEquals("[foo-kafka-0, foo-kafka-1, foo-kafka-2, foo-kafka-3, foo-kafka-4]", pre.toString());
                context.assertEquals("[foo-kafka-0, foo-kafka-1, foo-kafka-2, foo-kafka-3, foo-kafka-4]", post.toString());
            }
            async.complete();
        });
    }

    // TODO Precondition throws (e.g. times out)
    // TODO Rolling throws (e.g. times out) (should the post condition be called? Is it a BiFunction?
    // TODO Postcondition throws (e.g. times out)
    // TODO sort fn throws (e.g. times out)
}
