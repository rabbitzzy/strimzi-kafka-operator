/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.vertx.core.Future;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class RollerTest {

    private static final Logger log = LogManager.getLogger(RollerTest.class.getName());

    @Test
    public void testLeaderRolledLast(TestContext context) {
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
        Roller roller = new Roller(0, po,
            pod -> true,
            pod -> {
                pre.add(pod.getMetadata().getName());
                return Future.succeededFuture();
            },
            pod -> {
                post.add(pod.getMetadata().getName());
                return Future.succeededFuture();
            },
            x -> {
                if (x.get(0).equals(3)) {
                    log.debug("Pod 3 must be rolled last");
                    x.add(x.remove(0));
                }
                return Future.succeededFuture(x);
            }) {

            @Override
            protected Future<Void> restartPod(StatefulSet ss, Pod pod) {
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
        roller.maybeRollingUpdate(ss).setHandler(ar -> {
            if (ar.failed()) {
                context.fail(ar.cause());
            } else {
                context.assertEquals("[foo-kafka-0, foo-kafka-1, foo-kafka-2, foo-kafka-4, foo-kafka-3]", rolled.toString());
                context.assertEquals("[foo-kafka-0, foo-kafka-1, foo-kafka-2, foo-kafka-4, foo-kafka-3]", pre.toString());
                context.assertEquals("[foo-kafka-0, foo-kafka-1, foo-kafka-2, foo-kafka-4, foo-kafka-3]", post.toString());
            }
            async.complete();
        });
    }

    /**
     * Test when leadership changes during roll
     */
    @Test
    public void testLeaderChangesDuringRoll(TestContext context) {
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
        Roller roller = new Roller(0, po,
            pod -> true,
            pod -> {
                pre.add(pod.getMetadata().getName());
                return Future.succeededFuture();
            },
            pod -> {
                post.add(pod.getMetadata().getName());
                return Future.succeededFuture();
            },
                new Function<List<Integer>, Future<List<Integer>>>() {
                    boolean changedLeader = false;
                    @Override
                    public Future<List<Integer>> apply(List<Integer> x) {
                        int leader = !changedLeader ? 1 : 3;
                        if (x.get(0).equals(leader)) {
                            log.debug("Pod {} must be rolled last", leader);
                            x.add(x.remove(0));
                            changedLeader = true;
                        }
                        return Future.succeededFuture(x);
                    }
                }) {

            @Override
            protected Future<Void> restartPod(StatefulSet ss, Pod pod) {
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
        roller.maybeRollingUpdate(ss).setHandler(ar -> {
            if (ar.failed()) {
                context.fail(ar.cause());
            } else {
                context.assertEquals("[foo-kafka-0, foo-kafka-2, foo-kafka-4, foo-kafka-1, foo-kafka-3]", rolled.toString());
                context.assertEquals("[foo-kafka-0, foo-kafka-2, foo-kafka-4, foo-kafka-1, foo-kafka-3]", pre.toString());
                context.assertEquals("[foo-kafka-0, foo-kafka-2, foo-kafka-4, foo-kafka-1, foo-kafka-3]", post.toString());
            }
            async.complete();
        });
    }
    // TODO Precondition throws
    // TODO Rolling throws (e.g. times out) (should the post condition be called? Is it a BiFunction?
    // TODO Postcondition throws
}
