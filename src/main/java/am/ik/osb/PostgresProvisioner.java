package am.ik.osb;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.DeploymentList;
import io.fabric8.kubernetes.api.model.extensions.DoneableDeployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.ScalableResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import reactor.core.publisher.Mono;

import java.util.*;

import static java.util.Collections.singletonMap;

public class PostgresProvisioner {
    private final KubernetesClient kubernetesClient;

    public PostgresProvisioner(KubernetesClient kubernetesClient) {
        this.kubernetesClient = kubernetesClient;
    }

    public Mono<Void> provision(String instanceId, String namespace) {
        return createSecret(instanceId, namespace)
                .then(createPvc(instanceId, namespace))
                .then(createDeployment(instanceId, namespace))
                .then(createService(instanceId, namespace))
                .then();
    }

    public Mono<Boolean> isPodRunning(String instanceId, String namespace) {
        NonNamespaceOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> pods = this.kubernetesClient.pods().inNamespace(namespace);
        return Mono.fromCallable(() -> pods.withLabel("instance", instanceId)
                .list().getItems().stream()
                .anyMatch(i -> "Running".equals(i.getStatus().getPhase())));
    }

    public Mono<Void> deprovision(String instanceId, String namespace) {
        return deleteService(instanceId, namespace)
                .then(deleteDeployment(instanceId, namespace))
                .then(deletePvc(instanceId, namespace))
                .then(deleteSecret(instanceId, namespace))
                .then();
    }

    Mono<Secret> createSecret(String instanceId, String namespace) {
        NonNamespaceOperation<Secret, SecretList, DoneableSecret, Resource<Secret, DoneableSecret>> secrets = kubernetesClient.secrets().inNamespace(namespace);
        return Mono.fromCallable(() -> secrets.createNew()
                .withNewMetadata()
                /**/.withName("postgres-config-" + instanceId)
                /**/.withLabels(singletonMap("instance", instanceId))
                .endMetadata()
                .withType("Opaque")
                .withData(new HashMap<String, String>() {
                    {
                        put("postgres_user", Base64.getEncoder().encodeToString("admin".getBytes()));
                        put("postgres_password", Base64.getEncoder().encodeToString(UUID.randomUUID().toString().getBytes()));
                    }
                })
                .done());
    }

    Mono<Boolean> deleteSecret(String instanceId, String namespace) {
        NonNamespaceOperation<Secret, SecretList, DoneableSecret, Resource<Secret, DoneableSecret>> secrets = kubernetesClient.secrets().inNamespace(namespace);
        return Mono.fromCallable(() -> secrets.withLabel("instance", instanceId).delete());
    }

    Mono<Map<String, String>> getAdminCredentials(String instanceId, String namespace) {
        NonNamespaceOperation<Secret, SecretList, DoneableSecret, Resource<Secret, DoneableSecret>> secrets = kubernetesClient.secrets().inNamespace(namespace);
        return Mono.fromCallable(() -> {
            Secret secret = secrets.withName("postgres-config-" + instanceId).get();
            Map<String, String> decoded = new HashMap<>();
            secret.getData().forEach((k, v) -> decoded.put(k, new String(Base64.getDecoder().decode(v))));
            return decoded;
        });
    }

    boolean userExists(JdbcTemplate jdbcTemplate, String username) {
        Integer count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM pg_shadow WHERE usename=?", Integer.class, username);
        return count != null && count > 0;
    }

    Mono<Map<String, Object>> createUser(String instanceId, String bindingId, String namespace) {
        return this.getAdminCredentials(instanceId, namespace)
                .map(admin -> {
                    SingleConnectionDataSource dataSource = dataSource(instanceId, namespace, admin.get("postgres_user"), admin.get("postgres_password"));
                    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
                    Map<String, Object> credentials = new HashMap<>();
                    try {
                        String username = "u" + bindingId.replace("-", "");
                        if (userExists(jdbcTemplate, username)) {
                            throw new ConflictException(username + " already exists.");
                        }
                        String password = UUID.randomUUID().toString();
                        jdbcTemplate.execute("CREATE USER " + username + " WITH PASSWORD '" + password + "'");
                        credentials.put("jdbcUrl", dataSource.getUrl());
                        credentials.put("database", "db-" + instanceId);
                        credentials.put("username", username);
                        credentials.put("password", password);
                    } finally {
                        dataSource.destroy();
                    }
                    return credentials;
                });
    }

    Mono<Map<String, Object>> dropUser(String instanceId, String bindingId, String namespace) {
        return this.getAdminCredentials(instanceId, namespace)
                .map(admin -> {
                    SingleConnectionDataSource dataSource = dataSource(instanceId, namespace, admin.get("postgres_user"), admin.get("postgres_password"));
                    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
                    Map<String, Object> credentials = new HashMap<>();
                    try {
                        String username = "u" + bindingId.replace("-", "");
                        if (!userExists(jdbcTemplate, username)) {
                            throw new ConflictException(username + " does not exist.");
                        }
                        jdbcTemplate.execute("DROP USER " + username);
                    } finally {
                        dataSource.destroy();
                    }
                    return credentials;
                });
    }

    private SingleConnectionDataSource dataSource(String instanceId, String namespace, String username, String password) {
        List<NodeAddress> addresses = kubernetesClient.nodes().list().getItems().get(0)
                .getStatus()
                .getAddresses();
        String nodeIp = addresses.stream()
                .filter(a -> "ExternalIP".equals(a.getType()))
                .findFirst()
                .orElse(addresses.get(0))
                .getAddress();
        Integer nodePort = kubernetesClient.services().inNamespace(namespace)
                .withName("postgres-svc-" + instanceId)
                .get()
                .getSpec().getPorts()
                .get(0).getNodePort();
        String database = "db-" + instanceId;
        String jdbcUrl = "jdbc:postgresql://" + nodeIp + ":" + nodePort + "/" + database;
        SingleConnectionDataSource dataSource = new SingleConnectionDataSource(jdbcUrl, username, password, false);
        dataSource.setDriverClassName("org.postgresql.Driver");
        return dataSource;
    }

    Mono<PersistentVolumeClaim> createPvc(String instanceId, String namespace) {
        NonNamespaceOperation<PersistentVolumeClaim, PersistentVolumeClaimList, DoneablePersistentVolumeClaim, Resource<PersistentVolumeClaim, DoneablePersistentVolumeClaim>> pvcs = kubernetesClient.persistentVolumeClaims().inNamespace(namespace);
        return Mono.fromCallable(() -> pvcs.createNew()
                .withNewMetadata()
                /**/.withName("postgres-pv-claim-" + instanceId)
                /**/.withLabels(singletonMap("instance", instanceId))
                .endMetadata()
                .withNewSpec()
                /**/.withAccessModes("ReadWriteOnce")
                /**/.withNewResources()
                /*  */.withRequests(singletonMap("storage", new Quantity("1Gi")))
                /**/.endResources()
                .endSpec()
                .done());
    }

    Mono<Boolean> deletePvc(String instanceId, String namespace) {
        NonNamespaceOperation<PersistentVolumeClaim, PersistentVolumeClaimList, DoneablePersistentVolumeClaim, Resource<PersistentVolumeClaim, DoneablePersistentVolumeClaim>> pvcs = kubernetesClient.persistentVolumeClaims().inNamespace(namespace);
        return Mono.fromCallable(() -> pvcs.withLabel("instance", instanceId).delete());
    }

    Mono<Deployment> createDeployment(String instanceId, String namespace) {
        NonNamespaceOperation<Deployment, DeploymentList, DoneableDeployment, ScalableResource<Deployment, DoneableDeployment>> deployments = kubernetesClient.extensions().deployments().inNamespace(namespace);
        return Mono.fromCallable(() -> deployments.createNew()
                .withApiVersion("extensions/v1beta1")
                .withNewMetadata()
                /**/.withName("postgres-deploy-" + instanceId)
                /**/.withLabels(singletonMap("instance", instanceId))
                .endMetadata()
                .withNewSpec()
                /**/.withNewTemplate()
                /*  */.withNewMetadata()
                /*    */.withLabels(new HashMap<String, String>() {
                    {
                        put("app", "postgres");
                        put("instance", instanceId);
                    }
                })
                /*  */.endMetadata()
                /*  */.withNewSpec()
                /*    */.addNewVolume()
                /*      */.withName("postgres-storage-" + instanceId)
                /*      */.withNewPersistentVolumeClaim()
                /*        */.withClaimName("postgres-pv-claim-" + instanceId)
                /*      */.endPersistentVolumeClaim()
                /*    */.endVolume()
                /*    */.addNewContainer()
                /*      */.withName("postgres-" + instanceId)
                /*      */.withImage("postgres:9.6.3-alpine")
                /*      */.addNewEnv()
                /*        */.withName("POSTGRES_USER")
                /*        */.withNewValueFrom()
                /*          */.withNewSecretKeyRef("postgres_user", "postgres-config-" + instanceId, false)
                /*        */.endValueFrom()
                /*      */.endEnv()
                /*      */.addNewEnv()
                /*        */.withName("POSTGRES_PASSWORD")
                /*        */.withNewValueFrom()
                /*          */.withNewSecretKeyRef("postgres_password", "postgres-config-" + instanceId, false)
                /*        */.endValueFrom()
                /*      */.endEnv()
                /*      */.addNewEnv()
                /*        */.withName("POSTGRES_DB")
                /*        */.withValue("db-" + instanceId)
                /*      */.endEnv()
                /*      */.addNewEnv()
                /*        */.withName("PGDATA")
                /*        */.withValue("/var/lib/postgresql/data/pgdata")
                /*      */.endEnv()
                /*      */.addNewPort()
                /*        */.withName("postgres")
                /*        */.withContainerPort(5432)
                /*        */.withProtocol("TCP")
                /*      */.endPort()
                /*      */.addNewVolumeMount()
                /*        */.withName("postgres-storage-" + instanceId)
                /*        */.withMountPath("/var/lib/postgresql/data")
                /*      */.endVolumeMount()
                /*    */.endContainer()
                /*  */.endSpec()
                /**/.endTemplate()
                .endSpec()
                .done());
    }

    Mono<Boolean> deleteDeployment(String instanceId, String namespace) {
        NonNamespaceOperation<Deployment, DeploymentList, DoneableDeployment, ScalableResource<Deployment, DoneableDeployment>> deployments = kubernetesClient.extensions().deployments().inNamespace(namespace);
        return Mono.fromCallable(() -> deployments.withLabel("instance", instanceId).delete());
    }

    Mono<Service> createService(String instanceId, String namespace) {
        NonNamespaceOperation<Service, ServiceList, DoneableService, Resource<Service, DoneableService>> services = kubernetesClient.services().inNamespace(namespace);
        return Mono.fromCallable(() -> services.createNew()
                .withNewMetadata()
                /**/.withName("postgres-svc-" + instanceId)
                /**/.withLabels(singletonMap("instance", instanceId))
                .endMetadata()
                .withNewSpec()
                /**/.withType("NodePort")
                /**/.addNewPort()
                /*  */.withPort(5432)
                /**/.endPort()
                /**/.withSelector(new HashMap<String, String>() {
                    {
                        put("app", "postgres");
                        put("instance", instanceId);
                    }
                })
                .endSpec()
                .done());
    }

    Mono<Boolean> deleteService(String instanceId, String namespace) {
        NonNamespaceOperation<Service, ServiceList, DoneableService, Resource<Service, DoneableService>> services = kubernetesClient.services().inNamespace(namespace);
        return Mono.fromCallable(() -> services.withLabel("instance", instanceId).delete());
    }


    static class ConflictException extends RuntimeException {
        public ConflictException(String message) {
            super(message);
        }
    }
}
