/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.client;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DoneableStatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.batch.DoneableJob;
import io.fabric8.kubernetes.api.model.batch.Job;
import io.fabric8.kubernetes.api.model.batch.JobList;
import io.fabric8.kubernetes.api.model.rbac.KubernetesClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.KubernetesRoleBinding;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.ExecListener;
import io.fabric8.kubernetes.client.dsl.ExecWatch;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.fabric8.kubernetes.client.dsl.ScalableResource;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.test.k8s.NamespaceHolder;
import okhttp3.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Kubernetes extends NamespaceHolder {

    private static final Logger LOGGER = LogManager.getLogger(Kubernetes.class);
    protected final KubernetesClient client;
    protected String defaultNamespace;
    protected String namespace = NamespaceHolder.getNamespaceFromHolder() != null ? NamespaceHolder.getNamespaceFromHolder() : defaultNamespace;

    public Kubernetes(KubernetesClient client, String defaultNamespace) {
        this.client = client;
        this.defaultNamespace = defaultNamespace;
    }

    public KubernetesClient getClient() {
        return client;
    }

    public String getNamespace() {
        return namespace;
    }

    public void createNamespace(String name) {
        Namespace ns = new NamespaceBuilder().withNewMetadata().withName(name).endMetadata().build();
        client.namespaces().createOrReplace(ns);
    }

    public void deleteNamespace(String name) {
        client.namespaces().withName(name).delete();
    }

    public void deleteConfigMap(String configMapName) {
        client.configMaps().inNamespace(namespace).withName(configMapName).delete();
    }

    public ConfigMap getConfigMap(String configMapName) {
        return client.configMaps().inNamespace(namespace).withName(configMapName).get();
    }

    public boolean getConfigMapStatus(String configMapName) {
        return client.configMaps().inNamespace(namespace).withName(configMapName).isReady();
    }


    public String execInPod(String podName, String... command) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        LOGGER.info("Running command on pod {}: {}", podName, command);
        CompletableFuture<String> data = new CompletableFuture<>();
        try (ExecWatch execWatch = client.pods().inNamespace(namespace)
                .withName(podName)
                .readingInput(null)
                .writingOutput(baos)
                .usingListener(new ExecListener() {
                    @Override
                    public void onOpen(Response response) {
                        LOGGER.info("Reading data...");
                    }

                    @Override
                    public void onFailure(Throwable throwable, Response response) {
                        data.completeExceptionally(throwable);
                    }

                    @Override
                    public void onClose(int i, String s) {
                        data.complete(baos.toString());
                    }
                }).exec(command)) {
            return data.get(1, TimeUnit.MINUTES);
        } catch (Exception e) {
            LOGGER.warn("Exception running command {} on pod: {}", command, e.getMessage());
            return "";
        }
    }

    public String execInPodContainer(String podName, String container, String... command) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        LOGGER.info("Running command on pod {}: {}", podName, command);
        CompletableFuture<String> data = new CompletableFuture<>();
        try (ExecWatch execWatch = client.pods().inNamespace(namespace)
                .withName(podName).inContainer(container)
                .readingInput(null)
                .writingOutput(baos)
                .usingListener(new ExecListener() {
                    @Override
                    public void onOpen(Response response) {
                        LOGGER.info("Reading data...");
                    }

                    @Override
                    public void onFailure(Throwable throwable, Response response) {
                        data.completeExceptionally(throwable);
                    }

                    @Override
                    public void onClose(int i, String s) {
                        data.complete(baos.toString());
                    }
                }).exec(command)) {
            return data.get(1, TimeUnit.MINUTES);
        } catch (Exception e) {
            LOGGER.warn("Exception running command {} on pod: {}", command, e.getMessage());
            return "";
        }
    }

    public List<Pod> listPods(LabelSelector selector) {
        return client.pods().inNamespace(namespace).withLabelSelector(selector).list().getItems();
    }

    public List<Pod> listPods(Map<String, String> labelSelector) {
        return client.pods().inNamespace(namespace).withLabels(labelSelector).list().getItems();
    }

    public List<Pod> listPods() {
        return client.pods().inNamespace(namespace).list().getItems();
    }

    /**
     * Gets pod
     */
    public Pod getPod(String name) {
        return client.pods().inNamespace(namespace).withName(name).get();
    }

    /**
     * Deletes pod
     */
    public Boolean deletePod(Pod pod) {
        return client.pods().inNamespace(namespace).delete(pod);
    }

    public Date getCreationTimestampForPod(String podName) {
        DateFormat df = new SimpleDateFormat("yyyyMMdd'T'kkmmss'Z'");
        Pod pod = getPod(podName);
        Date parsedDate = null;
        try {
            df.parse(pod.getMetadata().getCreationTimestamp());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return parsedDate;
    }

    /**
     * Gets stateful set
     */
    public StatefulSet getStatefulSet(String statefulSetName) {
        return  client.apps().statefulSets().inNamespace(namespace).withName(statefulSetName).get();
    }

    /**
     * Gets stateful set
     */
    public RollableScalableResource<StatefulSet, DoneableStatefulSet> statefulSet(String statefulSetName) {
        return client.apps().statefulSets().inNamespace(namespace).withName(statefulSetName);
    }

    /**
     * Gets stateful set selectors
     */
    public LabelSelector getStatefulSetSelectors(String statefulSetName) {
        return client.apps().statefulSets().inNamespace(namespace).withName(statefulSetName).get().getSpec().getSelector();
    }

    /**
     * Gets stateful set status
     */
    public boolean getStatefulSetStatus(String statefulSetName) {
        return client.apps().statefulSets().inNamespace(namespace).withName(statefulSetName).isReady();
    }

    public void deleteStatefulSet(String statefulSetName) {
        client.apps().statefulSets().inNamespace(namespace).withName(statefulSetName).delete();
    }

    public Deployment createOrReplaceDeployment(Deployment deployment) {
        return client.apps().deployments().inNamespace(namespace).createOrReplace(deployment);
    }

    /**
     * Gets deployment
     */
    public Deployment getDeployment(String deploymentName) {
        return client.apps().deployments().inNamespace(namespace).withName(deploymentName).get();
    }

    /**
     * Gets deployment status
     */
    public LabelSelector getDeploymentSelectors(String deploymentName) {
        return client.apps().deployments().inNamespace(namespace).withName(deploymentName).get().getSpec().getSelector();
    }

    /**
     * Gets deployment status
     */
    public boolean getDeploymentStatus(String deploymentName) {
        return client.apps().deployments().inNamespace(namespace).withName(deploymentName).isReady();
    }

    public void deleteDeployment(String deploymentName) {
        client.apps().deployments().inNamespace(namespace).withName(deploymentName).delete();
    }

    /**
     * Gets deployment config status
     */
    public boolean getDeploymentConfigStatus(String deploymentCofigName) {
        return client.adapt(OpenShiftClient.class).deploymentConfigs().inNamespace(namespace).withName(deploymentCofigName).isReady();
    }

    public Secret createSecret(Secret secret) {
        return client.secrets().inNamespace(namespace).create(secret);
    }

    public Secret patchSecret(String secretName, Secret secret) {
        return client.secrets().inNamespace(namespace).withName(secretName).patch(secret);
    }


    public Secret getSecret(String secretName) {
        return client.secrets().inNamespace(namespace).withName(secretName).get();
    }

    public List<Secret> listSecrets() {
        return client.secrets().inNamespace(namespace).list().getItems();
    }

    public Service getService(String serviceName) {
        return client.services().inNamespace(namespace).withName(serviceName).get();
    }

    public boolean getServiceStatus(String serviceName) {
        return client.services().inNamespace(namespace).withName(serviceName).isReady();
    }

    public void deleteService(String serviceName) {
        client.services().inNamespace(namespace).withName(serviceName).delete();
    }

    public Job createJob(Job job) {
        return client.extensions().jobs().inNamespace(namespace).create(job);
    }

    public Job getJob(String jobName) {
        return client.extensions().jobs().inNamespace(namespace).withName(jobName).get();
    }

    public MixedOperation<Job, JobList, DoneableJob, ScalableResource<Job, DoneableJob>> listJobs() {
//    public MixedOperation<Job, JobList, DoneableJob, ScalableResource<Job, DoneableJob>> listJobs() {
//        return cmdClient.extensions().jobs().inNamespace(namespace);
        return client.extensions().jobs(); //TODO need namespace here
    }

    public String logs(String podName) {
        return client.pods().inNamespace(namespace).withName(podName).getLog();
    }

    public String logs(String podName, String containerName) {
        return client.pods().inNamespace(namespace).withName(podName).inContainer(containerName).getLog();
    }

    public List<Event> listEvents(String resourceType, String resourceName) {
        return client.events().inNamespace(namespace).list().getItems().stream()
                .filter(event -> event.getInvolvedObject().getKind().equals(resourceType))
                .filter(event -> event.getInvolvedObject().getName().equals(resourceName))
                .collect(Collectors.toList());
    }

    public KubernetesRoleBinding createOrReplaceKubernetesRoleBinding(KubernetesRoleBinding kubernetesRoleBinding) {
        return client.rbac().kubernetesRoleBindings().inNamespace(namespace).createOrReplace(kubernetesRoleBinding);
    }

    public KubernetesClusterRoleBinding createOrReplaceKubernetesClusterRoleBinding(KubernetesClusterRoleBinding kubernetesClusterRoleBinding) {
        return client.rbac().kubernetesClusterRoleBindings().inNamespace(namespace).createOrReplace(kubernetesClusterRoleBinding);
    }

    public Boolean deleteKubernetesClusterRoleBinding(KubernetesClusterRoleBinding kubernetesClusterRoleBinding) {
        return client.rbac().kubernetesClusterRoleBindings().inNamespace(namespace).delete(kubernetesClusterRoleBinding);
    }

    public <T extends HasMetadata, L extends KubernetesResourceList, D extends Doneable<T>> MixedOperation<T, L, D, Resource<T, D>> customResources(CustomResourceDefinition crd, Class<T> resourceType, Class<L> listClass, Class<D> doneClass) {
        return client.customResources(crd,resourceType, listClass, doneClass); //TODO namespace here
    }
}
