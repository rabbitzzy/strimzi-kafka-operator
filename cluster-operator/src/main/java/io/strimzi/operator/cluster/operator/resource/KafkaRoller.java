/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.stream.IntStream.range;

/**
 * <p>Manages the rolling restart of a Kafka cluster.</p>
 *
 * <p>The following algorithm is used:</p>
 *
 * <pre>
 * For each pod:
 *   1. Test whether the pod needs to be restarted.
 *       If not then:
 *         1. Continue to the next pod
 *   2. Otherwise, check whether the pod is the controller
 *       If so, and there are still pods to be maybe-rolled then:
 *         1. Add this pod to the end of the list
 *         2. Continue to the next pod
 *   3. Otherwise, check whether the pod can be restarted without "impacting availability"
 *       If not then:
 *         1. Add this pod to the end of the list
 *         2. Continue to the next pod
 *   4. Otherwise:
 *       1 Restart the pod
 *       2. Wait for it to become ready (in the kube sense)
 *       3. Continue to the next pod
 * </pre>
 *
 * <p>"impacting availability" is defined by {@link KafkaSorted}.</p>
 *
 * <p>Note this algorithm still works if there is a spontaneous
 * change in controller while the rolling restart is happening.</p>
 */
class KafkaRoller extends Roller<Integer, KafkaRoller.KafkaRollContext> {

    private static final Logger log = LogManager.getLogger(KafkaRoller.class.getName());

    private final PodOperator podOperations;
    private final long pollingIntervalMs;
    private final long operationTimeoutMs;
    private final Vertx vertx;

    public KafkaRoller(Vertx vertx, PodOperator podOperations,
                       long pollingIntervalMs, long operationTimeoutMs) {
        super(operationTimeoutMs, podOperations);
        this.vertx = vertx;
        this.podOperations = podOperations;
        this.pollingIntervalMs = pollingIntervalMs;
        this.operationTimeoutMs = operationTimeoutMs;
    }

    public static class KafkaRollContext implements Roller.Context<Integer> {

        private final List<Integer> pods;
        private final String namespace;
        private final String cluster;
        private final Secret clusterCaCertSecret;
        private final Secret coKeySecret;
        public AdminClient ac;

        public KafkaRollContext(String namespace, String cluster, List<Integer> pods, Secret clusterCaCertSecret, Secret coKeySecret) {
            this.namespace = namespace;
            this.cluster = cluster;
            this.pods = pods;
            this.clusterCaCertSecret = clusterCaCertSecret;
            this.coKeySecret = coKeySecret;
        }

        @Override
        public Integer next() {
            return pods.isEmpty() ? null : pods.remove(0);
        }

        @Override
        public String toString() {
            return pods.toString();
        }
    }

    @Override
    protected Future<KafkaRollContext> context(StatefulSet ss,
                                               Secret clusterCaCertSecret, Secret coKeySecret) {
        return Future.succeededFuture(new KafkaRollContext(
                ss.getMetadata().getNamespace(),
                Labels.cluster(ss),
                range(0, ss.getSpec().getReplicas()).boxed().collect(Collectors.toList()), clusterCaCertSecret, coKeySecret));
    }

    @Override
    protected Future<KafkaRollContext> sort(KafkaRollContext context, Predicate<Pod> podRestart) {
        return filterPods(context, podRestart)
            .compose(pod -> {
                if (pod != null) {
                    Future<KafkaRollContext> result = Future.future();
                    adminClient(context, pod)
                        .compose(i -> findNextRollable(context, podRestart))
                        .setHandler(ar -> {
                            close(context, result, ar);
                        });
                    return result;
                } else {
                    return Future.succeededFuture(context);
                }
            });
    }

    /**
     * Returns a Future which completes with an AdminClient instance.
     */
    protected Future<AdminClient> adminClient(KafkaRollContext context, Pod pod) {
        String hostname = KafkaCluster.podDnsName(context.namespace, context.cluster, pod.getMetadata().getName()) + ":" + KafkaCluster.REPLICATION_PORT;
        Future<AdminClient> result = Future.future();
        vertx.executeBlocking(
            f -> {
                try {
                    PasswordGenerator pg = new PasswordGenerator(12);
                    AdminClient ac;
                    String trustStorePassword = pg.generate();
                    File truststoreFile = setupTrustStore(trustStorePassword.toCharArray(), Ca.cert(context.clusterCaCertSecret, Ca.CA_CRT));
                    try {
                        String keyStorePassword = pg.generate();
                        File keystoreFile = setupKeyStore(context.coKeySecret,
                                keyStorePassword.toCharArray(),
                                Ca.cert(context.coKeySecret, "cluster-operator.crt"));
                        try {
                            Properties p = new Properties();
                            p.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, hostname);
                            p.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SSL");
                            p.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststoreFile.getAbsolutePath());
                            p.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, trustStorePassword);
                            p.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, trustStorePassword);
                            p.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystoreFile.getAbsolutePath());
                            p.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keyStorePassword);
                            p.setProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyStorePassword);
                            ac = AdminClient.create(p);
                        } finally {
                            keystoreFile.delete();
                        }
                    } finally {
                        truststoreFile.delete();
                    }
                    context.ac = ac;
                    f.complete(ac);
                } catch (Exception e) {
                    f.fail(e);
                }
            },
            result.completer());
        return result;
    }

    protected void close(KafkaRollContext context, Future<KafkaRollContext> result, AsyncResult<KafkaRollContext> ar) {
        AdminClient ac = context.ac;
        if (ac != null) {
            context.ac = null;
            vertx.executeBlocking(
                f -> {
                    try {
                        log.debug("Closing AC");
                        ac.close(10, TimeUnit.SECONDS);
                        log.debug("Closed AC");
                        f.complete();
                    } catch (Throwable t) {
                        log.debug(t);
                        f.fail(t);
                    }
                },
                fut -> {
                    if (ar.failed()) {
                        if (fut.failed()) {
                            ar.cause().addSuppressed(fut.cause());
                        }
                        result.fail(ar.cause());
                    } else if (fut.failed()) {
                        result.fail(fut.cause());
                    } else {
                        result.complete(ar.result());
                    }
                });
        }
    }

    protected Future<KafkaRollContext> filterAndFindNextRollable(KafkaRollContext context, Predicate<Pod> podRestart) {
        return filterPods(context, podRestart)
            .compose(pod -> {
                if (pod != null) {
                    return findNextRollable(context, podRestart);
                } else {
                    return Future.succeededFuture(context);
                }
            });
    }

    private Future<KafkaRollContext> findNextRollable(KafkaRollContext context, Predicate<Pod> podRestart) {
        // TODO how do we guarantee this algo terminates?
        // Right now it could bounce between the controller and a non-rollable non-broker indefinitely
        // It should probably:
        //   * Delay between each checking for rollablity of a given broker
        //   * (Eventually) give up when a broker is not rollable a number of times

        return controller(context.ac)
            .compose(controller -> {
                Integer podId = context.pods.get(0);
                if (podId.equals(controller) && context.pods.size() > 1) {
                    // Arrange to do the controller last when there are other brokers to be rolled
                    log.debug("Deferring restart of {} (it's the controller)", podId);
                    context.pods.add(context.pods.remove(0));
                    return filterAndFindNextRollable(context, podRestart);
                } else {
                    KafkaSorted ks = getKs(context);
                    return ks.canRoll(podId).compose(canRoll -> {
                        if (canRoll) {
                            // The first pod in the list needs rolling and is rollable: We're done
                            return Future.succeededFuture(context);
                        } else {
                            context.pods.add(context.pods.remove(0));
                            return filterAndFindNextRollable(context, podRestart);
                        }
                    });
                }
            });
    }

    protected KafkaSorted getKs(KafkaRollContext context) {
        return new KafkaSorted(context.ac);
    }


    /**
     * If {@link KafkaRollContext#pods} is empty then return a Future which succeeds with null.
     * Otherwise get the next pod from {@link KafkaRollContext#pods} and test it with the given podRestart.
     * If the pod needs to be restarted then complete the returned future with it.
     * Otherwise remove that pod from {@link KafkaRollContext#pods} and recurse.
     */
    private Future<Pod> filterPods(KafkaRollContext context, Predicate<Pod> podRestart) {
        if (context.pods.isEmpty()) {
            return Future.succeededFuture(null);
        } else {
            Integer podId = context.pods.get(0);
            String podName = KafkaCluster.kafkaPodName(context.cluster, podId);
            return podOperations.getAsync(context.namespace, podName).compose(pod -> {
                if (podRestart.test(pod)) {
                    log.debug("Pod {} needs to be restarted", podName);
                    return Future.succeededFuture(pod);
                } else {
                    // remove from pods and try next pod
                    log.debug("Pod {} does not need to be restarted", podName);
                    context.pods.remove(0);
                    return filterPods(context, podRestart);
                }
            });
        }
    }

    private File setupKeyStore(Secret clusterSecretKey, char[] password,
                                   X509Certificate clientCert) {
        Base64.Decoder decoder = Base64.getDecoder();

        try {
            KeyStore keyStore = KeyStore.getInstance("PKCS12");
            keyStore.load(null, password);
            Pattern parse = Pattern.compile("^---*BEGIN.*---*$(.*)^---*END.*---*$.*", Pattern.MULTILINE | Pattern.DOTALL);

            String keyText = new String(decoder.decode(clusterSecretKey.getData().get("cluster-operator.key")), StandardCharsets.ISO_8859_1);
            Matcher matcher = parse.matcher(keyText);
            if (!matcher.find()) {
                throw new RuntimeException("Bad client (CO) key. Key misses BEGIN or END markers");
            }
            PrivateKey clientKey = KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(
                    Base64.getMimeDecoder().decode(matcher.group(1))));

            keyStore.setEntry("cluster-operator",
                    new KeyStore.PrivateKeyEntry(clientKey, new Certificate[]{clientCert}),
                    new KeyStore.PasswordProtection(password));

            return store(password, keyStore);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private File setupTrustStore(char[] password, X509Certificate caCertCO) {

        try {
            KeyStore trustStore = null;
            trustStore = KeyStore.getInstance("PKCS12");
            trustStore.load(null, password);

            trustStore.setEntry(caCertCO.getSubjectDN().getName(), new KeyStore.TrustedCertificateEntry(caCertCO), null);
            return store(password, trustStore);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private File store(char[] password, KeyStore trustStore) throws Exception {
        File f = null;
        try {
            f = File.createTempFile(getClass().getName(), "ts");
            f.deleteOnExit();
            try (OutputStream os = new BufferedOutputStream(new FileOutputStream(f))) {
                trustStore.store(os, password);
            }
            return f;
        } catch (Exception e) {
            if (f != null && !f.delete()) {
                log.warn("Failed to delete temporary file in exception handler");
            }
            throw e;
        }
    }

    /**
     * Completes the returned future with the id of the controller of the cluster.
     * This will be -1 if there is not currently a controller.
     */
    Future<Integer> controller(AdminClient ac) {
        Future<Integer> result = Future.future();
        try {
            ac.describeCluster().controller().whenComplete((controllerNode, exception) -> {
                if (exception != null) {
                    result.fail(exception);
                } else {
                    int id = Node.noNode().equals(controllerNode) ? -1 : controllerNode.id();
                    log.debug("controller is {}", id);
                    result.complete(id);
                }
            });
        } catch (Throwable t) {
            result.fail(t);
        }
        return result;
    }

    @Override
    protected Future<Void> beforeRestart(Pod pod) {
        // The sort() method ensures that the given pod can be restarted without affecting the cluster.
        return Future.succeededFuture();
    }

    @Override
    protected Future<Void> afterRestart(Pod pod) {
        String namespace = pod.getMetadata().getNamespace();
        String podName = pod.getMetadata().getName();
        return podOperations.readiness(namespace, podName, pollingIntervalMs, operationTimeoutMs)
            .recover(error -> {
                log.warn("Error waiting for pod {}/{} to become ready: {}", namespace, podName, error);
                return Future.failedFuture(error);
            });
    }

    @Override
    protected Future<Pod> pod(StatefulSet ss, Integer podId) {
        return podOperations.getAsync(ss.getMetadata().getNamespace(), ss.getMetadata().getName() + "-" + podId);
    }

}
