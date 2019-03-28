/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.vertx.core.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * <p>Abstracted algorithm for rolling the pods of a StatefulSet.</p>
 *
 * <p>The algorithm works as follows:</p>
 * <pre>
 * 1. Get a (mutable) context to be passed around during the
 *    roll (by applying {@link #context(StatefulSet, Secret, Secret)}.
 *    This context is implementation dependent.
 * 2. {@link Context#sort(Context, Predicate)} the context. This could,
 *    for example, influence the order in which pods and rolled and when pods are rolled.
 * 3. While {@link Context#next()} returns a non-null value:
 *      a. Precondition
 *      b. Restart
 *      c. Postcondition
 *      d. Continue from step 2.
 * </pre>
 *
 * @param <P> The class representing pods.
 *           E.g. could be {@code Pod}, or {@code String} (pod name), or {@code Integer} (pod number).
 * @param <C> A context for the roll, passed along during the rolling restart.
 */
abstract class Roller<P, C extends Roller.Context<P>> {

    private final Logger log = LogManager.getLogger(getClass());

    private static final String NO_UID = "NULL";
    protected final long operationTimeoutMs;
    protected final PodOperator podOperations;

    public Roller(long operationTimeoutMs, PodOperator podOperations) {
        this.operationTimeoutMs = operationTimeoutMs;
        this.podOperations = podOperations;
    }

    public interface Context<P> {

        /** @return the next pod to be rolled, or null if there are no more pods to roll */
        P next();

        String toString();
    }

    /**
     * Get the rolling context for restarting the pods in the given StatefulSet.
     */
    protected abstract Future<C> context(StatefulSet ss,
                               Secret clusterCaCertSecret, Secret coKeySecret);

    /**
     * Sort the context, potentially changing the next pod to be rolled.
     */
    protected abstract Future<C> sort(C context, Predicate<Pod> podRestart);

    /**
     * A future which should complete when the given pod is ready to be rolled
     */
    protected abstract Future<Void> beforeRestart(Pod pod);

    /**
     * A future which should complete when the given pod is "ready" after being rolled.
     */
    protected abstract Future<Void> afterRestart(Pod pod);

    protected abstract Future<Pod> pod(StatefulSet ss, P pod);

    private static String getPodUid(Pod resource) {
        if (resource == null || resource.getMetadata() == null) {
            return NO_UID;
        }
        return resource.getMetadata().getUid();
    }

    /**
     * Get list of all pods (by applying {@link #context(StatefulSet, Secret, Secret)}.
     * While collection not empty
     * {@linkplain #sort(Context, Predicate) sort the collection} and get the first element
     * For this element:
     *   Does pod need restart?
     *   Is size(collection) > 1 and pod "leader":
     *     Add to end of queue
     *   Else:
     *     Precondition
     *     Restart
     *     Postcondition
     */
    public Future<Void> rollingRestart(StatefulSet ss,
                                Secret clusterCaCertSecret, Secret coKeySecret, Predicate<Pod> podRestart) {
        return context(ss, clusterCaCertSecret, coKeySecret).compose(new Function<C, Future<C>>() {
                @Override
                public Future<C> apply(C context) {
                    return sort(context, podRestart).recover(error -> {
                        log.warn("Error when determining next pod to roll", error);
                        return Future.succeededFuture(context);
                    }).compose(currentContext -> {
                        P pod = context.next();
                        if (pod == null) {
                            return Future.succeededFuture();
                        } else {
                            return pod(ss, pod).compose(p -> {
                                log.debug("Rolling pod {} (still to consider: {})", p.getMetadata().getName(), currentContext);
                                Future<Void> f = restartWithCallbacks(ss, p);
                                return f.map(i -> currentContext).compose(this);
                            });
                        }
                    });
                }
            }).map((Void) null);
    }


    /**
     * Asynchronously apply the before restart callback, then restart the given pod
     * by deleting it and letting it be recreated by K8s, then apply the after restart callback.
     * Return a Future which completes when the after restart callback for the given pod has completed.
     * @param ss The StatefulSet.
     * @param pod The Pod to restart.
     * @return a Future which completes when the after restart callback for the given pod has completed.
     */
    private Future<Void> restartWithCallbacks(StatefulSet ss, Pod pod) {
        String namespace = ss.getMetadata().getNamespace();
        //String name = ss.getMetadata().getName();
        String podName = pod.getMetadata().getName();
        Future<Void> fut;
        //log.debug("Testing whether pod {} needs roll", podName);
        //if (podRestart.test(pod)) {
        log.debug("Precondition on pod {}", podName);
        fut = beforeRestart(pod).compose(i -> {
            log.debug("Rolling pod {}", podName);
            return restart(ss, pod);
        }).compose(i -> {
            String ssName = podName.substring(0, podName.lastIndexOf('-'));
            log.debug("Rolling update of {}/{}: wait for pod {} postcondition", namespace, ssName, podName);
            return afterRestart(pod);
        });
        return fut;

    }

    /**
     * Asynchronously delete the given pod, return a Future which completes when the Pod has been recreated.
     * Note: The pod might not be "ready" when the returned Future completes.
     * @param ss The StatefulSet
     * @param pod The pod to be restarted
     * @return a Future which completes when the Pod has been recreated
     */
    protected Future<Void> restart(StatefulSet ss, Pod pod) {
        long pollingIntervalMs = 1_000;
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
                    Future<Void> del = podOperations.waitFor(namespace, name, pollingIntervalMs, operationTimeoutMs, (ignore1, ignore2) -> {
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
