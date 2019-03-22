/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.stream.IntStream.range;

public class Roller {

    private static final Logger log = LogManager.getLogger(Roller.class.getName());

    private static final String NO_UID = "NULL";
    private long operationTimeoutMs;
    private PodOperator podOperations;
    private Predicate<Pod> podRestart;
    private Function<Pod, Future<Void>> preCondition;
    private Function<Pod, Future<Void>> postCondition;
    private final Function<List<Integer>, Future<List<Integer>>> sort;

    public Roller(long operationTimeoutMs, PodOperator podOperations,
                  Predicate<Pod> podRestart, Function<Pod, Future<Void>> preCondition, Function<Pod, Future<Void>> postCondition,
                  Function<List<Integer>, Future<List<Integer>>> sort) {
        this.operationTimeoutMs = operationTimeoutMs;
        this.podOperations = podOperations;
        this.podRestart = podRestart;
        this.preCondition = preCondition;
        this.postCondition = postCondition;
        this.sort = sort;
    }

    private static String getPodUid(Pod resource) {
        if (resource == null || resource.getMetadata() == null) {
            return NO_UID;
        }
        return resource.getMetadata().getUid();
    }

    /**
     * Asynchronously apply the given {@code podRestart}, if it returns true then restart the pod
     * given by {@code podName} by deleting it and letting it be recreated by K8s;
     * in any case return a Future which completes when the given (possibly recreated) pod is ready.
     * @param ss The StatefulSet.
     * @param podName The name of the Pod to possibly restart.
     * @param podRestart The predicate for deciding whether to restart the pod.
     * @return a Future which completes when the given (possibly recreated) pod is ready.
     */
    Future<Void> maybeRestartPod(StatefulSet ss, String podName, Predicate<Pod> podRestart,
                                 Function<Pod, Future<Void>> preCondition,
                                 Function<Pod, Future<Void>> postCondition) {
        long pollingIntervalMs = 1_000;
        long timeoutMs = operationTimeoutMs;
        String namespace = ss.getMetadata().getNamespace();
        String name = ss.getMetadata().getName();
        return podOperations.getAsync(ss.getMetadata().getNamespace(), podName).compose(pod -> {
            Future<Void> fut;
            log.debug("Testing whether pod {} needs roll", podName);
            if (podRestart.test(pod)) {
                log.debug("Precondition on pod {}", podName);
                fut = preCondition.apply(pod).compose(i -> {
                    log.debug("Rolling pod {}", podName);
                    return restartPod(ss, pod);
                }).compose(i -> {
                    log.debug("Postcondition on pod {}", podName);
                    return postCondition.apply(pod);
                });
            } else {
                log.debug("Rolling update of {}/{}: pod {} no need to roll", namespace, name, podName);
                fut = Future.succeededFuture();
            }
            return fut; /*.compose(ignored -> {
                log.debug("Rolling update of {}/{}: wait for pod {} readiness", namespace, name, podName);
                return podOperations.readiness(namespace, podName, pollingIntervalMs, timeoutMs);
            });*/
        });
    }

    /**
     * Asynchronously delete the given pod, return a Future which completes when the Pod has been recreated.
     * Note: The pod might not be ready when the returned Future completes.
     * @param ss The StatefulSet
     * @param pod The pod to be restarted
     * @return a Future which completes when the Pod has been recreated
     */
    protected Future<Void> restartPod(StatefulSet ss, Pod pod) {
        long pollingIntervalMs = 1_000;
        long timeoutMs = operationTimeoutMs;
        String namespace = ss.getMetadata().getNamespace();
        String name = ss.getMetadata().getName();
        String podName = pod.getMetadata().getName();
        Future<Void> deleteFinished = Future.future();
        log.info("Rolling update of {}/{}: Rolling pod {}", namespace, name, podName);

        // Determine generation of deleted pod
        String deleted = getPodUid(pod);

        // Delete the pod
        log.debug("Rolling update of {}/{}: Waiting for pod {} to be deleted", namespace, name, podName);
        Future<Void> podReconcileFuture =
                podOperations.reconcile(namespace, podName, null).compose(ignore -> {
                    Future<Void> del = podOperations.waitFor(namespace, name, pollingIntervalMs, timeoutMs, (ignore1, ignore2) -> {
                        // predicate - changed generation means pod has been updated
                        String newUid = getPodUid(podOperations.get(namespace, podName));
                        boolean done = !deleted.equals(newUid);
                        if (done) {
                            log.debug("Rolling pod {} finished", podName);
                        }
                        return done;
                    });
                    return del;
                });

        podReconcileFuture.setHandler(deleteResult -> {
            if (deleteResult.succeeded()) {
                log.debug("Rolling update of {}/{}: Pod {} was deleted", namespace, name, podName);
            }
            deleteFinished.handle(deleteResult);
        });
        return deleteFinished;
    }

    Future<Void> maybeRollingUpdate(StatefulSet ss) {

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
        Function<List<Integer>, Future<List<Integer>>> listFutureFunction = new Function<List<Integer>, Future<List<Integer>>>() {
            @Override
            public Future<List<Integer>> apply(List<Integer> unsortedPodIds) {
                return sort.apply(unsortedPodIds).compose(podIds -> {
                    if (podIds.size() == 1) {
                        int podId = podIds.get(0);
                        String podName = ss.getMetadata().getName() + "-" + podId;
                        return maybeRestartPod(ss, podName, podRestart, preCondition, postCondition).compose(i -> Future.succeededFuture());
                    } else {
                        int podId = podIds.remove(0);
                        String podName = ss.getMetadata().getName() + "-" + podId;
                        Future<Void> f = maybeRestartPod(ss, podName, podRestart, preCondition, postCondition);
                        return f.map(i -> podIds).compose(this);
                    }
                });
            }
        };
        return Future.succeededFuture(range(0, ss.getSpec().getReplicas()).boxed().collect(Collectors.toList()))
                .compose(listFutureFunction).map((Void) null);
    }

    /**
     * Returning a pod ordering which ensures that a certain "leader" pod is the last to be rolled.
     * @param isLeaderFn Function which determines leadership
     * @return
     */
    private static Function<List<Integer>, Future<List<Integer>>> leaderLastOrder(Function<Integer, Future<Boolean>> isLeaderFn) {
        return podIds -> {
            if (podIds.size() <= 1) {
                return Future.succeededFuture(podIds);
            } else {
                Integer podId = podIds.get(0);
                return isLeaderFn.apply(podId).compose(isLeader -> {
                    if (isLeader) {
                        log.debug("Deferring possible roll of pod {}", podId);
                        List<Integer> result = new ArrayList<>(podIds);
                        result.add(result.remove(0));
                        return Future.succeededFuture(result);
                    } else {
                        return Future.succeededFuture(podIds);
                    }
                });
            }
        };
    }

    /**
     * Returns a Future which completes with whether the given pod is the Controller.
     * @param podId
     * @return
     */
    protected static Future<Boolean> isController(int podId) {
        // TODO
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

    static Roller kafkaRoller(PodOperator podOperations, Predicate<Pod> podRestart) {
        long pollingIntervalMs = 0;
        long operationTimeoutMs = 0;
        return new Roller(0, podOperations, podRestart,
            pod -> {
                return Future.succeededFuture();
            },
            pod -> {
                String namespace = pod.getMetadata().getNamespace();
                String podName = pod.getMetadata().getName();
                String ssName = podName.substring(0, podName.lastIndexOf('-'));
                log.debug("Rolling update of {}/{}: wait for pod {} readiness", namespace, ssName, podName);
                return podOperations.readiness(namespace, podName, pollingIntervalMs, operationTimeoutMs);
            },
            leaderLastOrder(Roller::isController));
    }

    static Roller zookeeperRoller(PodOperator podOperations, Predicate<Pod> podRestart) {
        long pollingIntervalMs = 0;
        long operationTimeoutMs = 0;
        Vertx vertx = null;
        SecretOperator secretOperator = null;
        Supplier< BackOff > backOffSupplier = null;
        String cluster, String namespace, List<Pod> pods, Secret coKeySecret
        return new Roller(operationTimeoutMs, podOperations, podRestart,
            pod -> {
                return Future.succeededFuture();
            },
            pod -> { // post condition
                String namespace = pod.getMetadata().getNamespace();
                String podName = pod.getMetadata().getName();
                String ssName = podName.substring(0, podName.lastIndexOf('-'));
                log.debug("Rolling update of {}/{}: wait for pod {} readiness", namespace, ssName, podName);
                return podOperations.readiness(namespace, podName, pollingIntervalMs, operationTimeoutMs);
            },
            leaderLastOrder(podId -> {
                return new ZookeeperLeaderFinder(vertx, secretOperator, backOffSupplier)
                        .findZookeeperLeader()
                        .map(leader -> leader == podId);
            }));
    }
}
