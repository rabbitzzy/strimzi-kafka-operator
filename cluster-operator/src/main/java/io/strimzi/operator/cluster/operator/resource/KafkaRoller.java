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
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.stream.IntStream.range;

class KafkaRoller extends Roller<Integer, KafkaRoller.KafkaRollContext> {

    private static final Logger log = LogManager.getLogger(KafkaRoller.class.getName());

    private final PodOperator podOperations;
    private final long pollingIntervalMs;
    private final long operationTimeoutMs;
    private final Secret clusterCaCertSecret;
    private final Secret coKeySecret;

    public KafkaRoller(PodOperator podOperations, Predicate<Pod> podRestart,
                       long pollingIntervalMs, long operationTimeoutMs,
                       Secret clusterCaCertSecret, Secret coKeySecret) {
        super(0, podOperations, podRestart);
        this.podOperations = podOperations;
        this.pollingIntervalMs = pollingIntervalMs;
        this.operationTimeoutMs = operationTimeoutMs;
        this.clusterCaCertSecret = clusterCaCertSecret;
        this.coKeySecret = coKeySecret;
    }

    public static class KafkaRollContext implements Roller.Context<Integer> {

        private final List<Integer> pods;
        private final String namespace;
        private final String cluster;

        public KafkaRollContext(String namespace, String cluster, List<Integer> pods) {
            this.namespace = namespace;
            this.cluster = cluster;
            this.pods = pods;
        }

        @Override
        public Integer next() {
            return pods.remove(0);
        }

        @Override
        public boolean isEmpty() {
            return pods.isEmpty();
        }
    }

    @Override
    Future<KafkaRollContext> context(StatefulSet ss) {
        return Future.succeededFuture(new KafkaRollContext(
                ss.getMetadata().getNamespace(),
                Labels.cluster(ss),
                range(0, ss.getSpec().getReplicas()).boxed().collect(Collectors.toList())));
    }

    @Override
    Future<KafkaRollContext> sort(KafkaRollContext context) {
        if (context.pods.size() <= 1) {
            // If there's a single pod left it's the controller so we need to rol it anyway
            // TODO but we might need to wait for it to be available to roll according to can roll
            // TODO think about retry here and in the branch below
            return Future.succeededFuture(context);
        } else {
            Integer podId = context.pods.get(0);
            String hostname = KafkaCluster.podDnsName(context.namespace, context.cluster, podId) + ":" + KafkaCluster.REPLICATION_PORT;
            return adminClient(hostname).compose(ac -> {
                Future<KafkaRollContext> f = Future.future();
                return controller(ac).compose(controller -> {
                    ArrayList<Integer> podsToRollExcludingController = new ArrayList<>(context.pods);
                    podsToRollExcludingController.remove(controller);
                    KafkaSorted ks = new KafkaSorted(ac);
                    return findRollableBroker(podsToRollExcludingController, ks::canRoll, 60_000, 3_600_000).map(brokerId -> {
                        int index = context.pods.indexOf(brokerId);
                        context.pods.add(0, context.pods.remove(index));
                        return context;
                    });
                }).map(r -> {
                    ac.close();
                    return r;
                }).recover(e -> {
                    ac.close();
                    return Future.failedFuture(e);
                });
            });
        }
    }

    private Future<AdminClient> adminClient(String bootstrapBroker) {
        // TODO TLS
        Vertx vertx = null;
        Future<AdminClient> result = Future.future();
        vertx.executeBlocking(f -> {
            try {
                PasswordGenerator pg = new PasswordGenerator(12);
                AdminClient ac;
                String trustStorePassword = pg.generate();
                File truststoreFile = setupTrustStore(trustStorePassword.toCharArray(), Ca.cert(clusterCaCertSecret, Ca.CA_CRT));
                try {
                    String keyStorePassword = pg.generate();
                    File keystoreFile = setupKeyStore(coKeySecret,
                            keyStorePassword.toCharArray(),
                            Ca.cert(coKeySecret, "cluster-operator.crt"));
                    try {
                        Properties p = new Properties();
                        p.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBroker);
                        p.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststoreFile.getAbsolutePath());
                        p.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, trustStorePassword);
                        p.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystoreFile.getAbsolutePath());
                        p.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keyStorePassword);
                        ac = AdminClient.create(p);
                    } finally {
                        keystoreFile.delete();
                    }
                } finally {
                    truststoreFile.delete();
                }
                f.complete(ac);
            } catch (Exception e) {
                f.fail(e);
            }
        },
            result.completer());
        return result;
    }

    private File setupKeyStore(Secret clusterSecretKey, char[] password,
                                   X509Certificate clientCert) {
        Base64.Decoder decoder = Base64.getDecoder();

        try {
            KeyStore keyStore = KeyStore.getInstance("PKCS12");
            keyStore.load(null, password);
            Pattern parse = Pattern.compile("^---*BEGIN.*---*$(.*)^---*END.*---*$.*", Pattern.MULTILINE | Pattern.DOTALL);

            String keyText = new String(decoder.decode(clusterSecretKey.getData().get("cluster-operator.crt")), StandardCharsets.ISO_8859_1);
            Matcher matcher = parse.matcher(keyText);
            if (!matcher.find()) {
                throw new RuntimeException("Bad client (CO) key. Key misses BEGIN or END markers");
            }
            PrivateKey clientKey = KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(
                    Base64.getMimeDecoder().decode(matcher.group(1))));

            keyStore.setEntry("tls-probe",
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

    private File store(char[] password, KeyStore trustStore) throws IOException, KeyStoreException, NoSuchAlgorithmException, CertificateException {
        File f = File.createTempFile(getClass().getName(), "ts");
        try {
            f.deleteOnExit();
            try (OutputStream os = new BufferedOutputStream(new FileOutputStream(f))) {
                trustStore.store(os, password);
            }
            return f;
        } catch (Exception e) {
            f.delete();
            throw e;
        }
    }


    Future<Integer> controller(AdminClient ac) {
        Future<Integer> result = Future.future();
        ac.describeCluster().controller().whenComplete((controllerNode, exception) -> {
            if (exception != null) {
                result.fail(exception);
            } else {
                result.complete(controllerNode.id());
            }
        });
        return result;
    }

    ////////////////////////////////////////////
    /*


    Future<Integer> leader2(String bootstrapBroker) {
        // TODO retry
        // TODO TLS
        /*
        1. We nee
         * /
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
    */
    //////////////////////////////////////////

    /**
     * Find the first broker in the given {@code brokers} which is rollable
     * according to XXX.
     * TODO: If there are none then retry every P seconds.
     * TODO: Wait up to T seconds before returning the first broker in the list.
     * @param brokers
     * @return
     */
    Future<Integer> findRollableBroker(List<Integer> brokers, Function<Integer, Future<Boolean>> rollable, long pollMs, long timeoutMs) {
        Future<Integer> result = Future.future();
        long deadline = System.currentTimeMillis() + timeoutMs;
        Vertx vertx = null;
        Handler<Long> handler = new Handler<Long>() {

            @Override
            public void handle(Long event) {
                findRollableBroker(brokers, rollable).map(brokerId -> {
                    if (brokerId != -1) {
                        result.complete(brokerId);
                    } else {
                        if (System.currentTimeMillis() > deadline) {
                            result.complete(brokers.get(0));
                        } else {
                            // TODO vertx.setTimer(pollMs, this);
                        }
                    }
                    return null;
                });
            }
        };
        handler.handle(null);
        return result;
    }

    Future<Integer> findRollableBroker(List<Integer> brokers, Function<Integer, Future<Boolean>> rollable) {
        Future<Integer> result = Future.future();
        Future<Iterator<Integer>> f = Future.succeededFuture(brokers.iterator());
        Function<Iterator<Integer>, Future<Iterator<Integer>>> fn = new Function<Iterator<Integer>, Future<Iterator<Integer>>>() {
            @Override
            public Future<Iterator<Integer>> apply(Iterator<Integer> iterator) {
                if (iterator.hasNext()) {
                    Integer brokerId = iterator.next();
                    return rollable.apply(brokerId).compose(canRoll -> {
                        if (canRoll) {
                            result.complete(brokerId);
                            return Future.succeededFuture();
                        }
                        return Future.succeededFuture(iterator).compose(this);
                    });
                } else {
                    result.complete(-1);
                    return Future.succeededFuture();
                }
            }
        };
        f.compose(fn);
        return result;
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
