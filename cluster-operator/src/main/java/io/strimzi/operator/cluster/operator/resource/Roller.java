/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.vertx.core.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Abstracted algorithm for rolling the pods of a StatefulSet.
 *
 * The algorithm works as follows:
 * <pre>
 * 1. Get all pods to be rolled (by applying {@link #context(StatefulSet)}.
 * 2. While there are still pods to roll (i.e. {@link Context#size()} &gt; 1)
 *    1. {@linkplain #sort(Context) sort the collection} and get the first element
 *    2. For this element:
 *      1. Does pod need restart?
 *        1. Is size(collection) &gt; 1 and pod "leader":
 *          1. Add to end of queue
 *        2. Else:
 *          1. Precondition
 *          2. Restart
 *          3. Postcondition
 * </pre>
 * @param <P> The class representing pods.
 *           E.g. could be {@code Pod}, or {@code String} (pod name), or {@code Integer} (pod number).
 * @param <C> A context for the roll, passed along during the rolling restart.
 */
public abstract class Roller<P, C extends Roller.Context<P>> {

    private final Logger log = LogManager.getLogger(getClass());

    private static final String NO_UID = "NULL";
    protected final long operationTimeoutMs;
    protected final PodOperator podOperations;
    protected final Predicate<Pod> podRestart;

    public Roller(long operationTimeoutMs, PodOperator podOperations,
                  Predicate<Pod> podRestart) {
        this.operationTimeoutMs = operationTimeoutMs;
        this.podOperations = podOperations;
        this.podRestart = podRestart;
    }

    interface Context<P> {
        P next();
        int size();
    }

    static class ListContext<P> implements Context<P> {

        private final List<P> pods;
        private final List<P> allPods;

        ListContext(List<P> pods) {
            this.allPods = Collections.unmodifiableList(new ArrayList<>(pods));
            this.pods = new ArrayList<>(pods);
        }

        @Override
        public P next() {
            return pods.remove(0);
        }

        @Override
        public int size() {
            return pods.size();
        }

        public List<P> remainingPods() {
            return Collections.unmodifiableList(pods);
        }

        public List<P> allPods() {
            return allPods;
        }

        public void addLast(P pod) {
            pods.add(pod);
        }

        public String toString() {
            return pods.toString();
        }
    }

    /**
     * Get the rolling context for restarting the pods in the given StatefulSet.
     */
    abstract Future<C> context(StatefulSet ss);

    /**
     * Sort the context, potentially changing the next pod to be rolled.
     */
    abstract Future<C> sort(C context);

    /**
     * A future which should complete when the given pod is ready to be rolled
     */
    abstract Future<Void> beforeRestart(Pod pod);

    /**
     * A future which should complete when the given pod is "ready" after being rolled.
     */
    abstract Future<Void> afterRestart(Pod pod);

    abstract String podName(StatefulSet ss, P pod);

    private static String getPodUid(Pod resource) {
        if (resource == null || resource.getMetadata() == null) {
            return NO_UID;
        }
        return resource.getMetadata().getUid();
    }

    /**
     * Get list of all pods (by applying {@link #context(StatefulSet)}.
     * While collection not empty
     * {@linkplain #sort(Context) sort the collection} and get the first element
     * For this element:
     *   Does pod need restart?
     *   Is size(collection) > 1 and pod "leader":
     *     Add to end of queue
     *   Else:
     *     Precondition
     *     Restart
     *     Postcondition
     */
    Future<Void> maybeRollingUpdate(StatefulSet ss) {
        return context(ss).compose(new Function<C, Future<C>>() {
                @Override
                public Future<C> apply(C context) {
                    Function<C, Future<C>> fn = pods -> {
                        log.debug("Still to maybe roll: {}", pods);
                        P pod = context.next();
                        String podName = podName(ss, pod);
                        Future<Void> f = maybeRestartPod(ss, podName);
                        if (context.size() == 0) {
                            return f.compose(i -> Future.succeededFuture());
                        } else {
                            return f.map(i -> pods).compose(this);
                        }
                    };
                    return sort(context).recover(error -> {
                        log.warn("Error when determining next pod to roll", error);
                        return Future.succeededFuture(context);
                    }).compose(fn);
                }
            }).map((Void) null);
    }


    /**
     * Asynchronously apply the given {@code podRestart}, if it returns true then restart the pod
     * given by {@code podName} by deleting it and letting it be recreated by K8s;
     * in any case return a Future which completes when the given (possibly recreated) pod is ready.
     * @param ss The StatefulSet.
     * @param podName The name of the Pod to possibly restart.
     * @return a Future which completes when the given (possibly recreated) pod is ready.
     */
    Future<Void> maybeRestartPod(StatefulSet ss, String podName) {
        String namespace = ss.getMetadata().getNamespace();
        String name = ss.getMetadata().getName();
        return podOperations.getAsync(ss.getMetadata().getNamespace(), podName).compose(pod -> {
            Future<Void> fut;
            log.debug("Testing whether pod {} needs roll", podName);
            if (podRestart.test(pod)) {
                log.debug("Precondition on pod {}", podName);
                fut = beforeRestart(pod).compose(i -> {
                    log.debug("Rolling pod {}", podName);
                    return restartPod(ss, pod);
                }).compose(i -> {
                    String ssName = podName.substring(0, podName.lastIndexOf('-'));
                    log.debug("Rolling update of {}/{}: wait for pod {} postcondition", namespace, ssName, podName);
                    return afterRestart(pod);
                });
            } else {
                log.debug("Rolling update of {}/{}: pod {} no need to roll", namespace, name, podName);
                fut = Future.succeededFuture();
            }
            return fut;
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
}
