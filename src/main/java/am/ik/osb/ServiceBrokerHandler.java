package am.ik.osb;

import am.ik.osb.PostgresProvisioner.ConflictException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.util.CollectionUtils;
import org.springframework.web.reactive.function.server.HandlerFunction;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import org.yaml.snakeyaml.Yaml;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static am.ik.osb.ServiceBrokerHandler.ProvisioningState.*;
import static java.util.Collections.singletonMap;
import static org.springframework.web.reactive.function.server.RequestPredicates.*;
import static org.springframework.web.reactive.function.server.RouterFunctions.route;
import static org.springframework.web.reactive.function.server.ServerResponse.ok;
import static org.springframework.web.reactive.function.server.ServerResponse.status;

public class ServiceBrokerHandler {
    private static final Logger log = LoggerFactory.getLogger(ServiceBrokerHandler.class);
    private final PostgresProvisioner provisioner;
    private final String namespace;
    private final Object catalog;
    private final Map<String, ProvisioningState> stateMap = new ConcurrentHashMap<>();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Mono<ServerResponse> badRequest = Mono.defer(() -> status(HttpStatus.BAD_REQUEST)
            .syncBody(this.objectMapper.createObjectNode().put("description", "Bad Request.")));

    public ServiceBrokerHandler(PostgresProvisioner provisioner, String namespace, Resource catalog) throws IOException {
        this.provisioner = provisioner;
        this.namespace = namespace;
        Yaml yaml = new Yaml();
        try (InputStream stream = catalog.getInputStream()) {
            this.catalog = yaml.load(stream);
        }
    }

    public RouterFunction<ServerResponse> routes() {
        final String serviceInstance = "/v2/service_instances/{instanceId}";
        final String serviceBindings = "/v2/service_instances/{instanceId}/service_bindings/{bindingId}";
        return route(GET("/v2/catalog"), this::catalog)
                .andRoute(PUT(serviceInstance), this::provisioning)
                .andRoute(PATCH(serviceInstance), this::update)
                .andRoute(GET(serviceInstance + "/last_operation"), this::lastOperation)
                .andRoute(DELETE(serviceInstance), this::deprovisioning)
                .andRoute(PUT(serviceBindings), this::bind)
                .andRoute(DELETE(serviceBindings), this::unbind)
                .filter(this::versionCheck)
                .filter(this::basicAuthentication);
    }

    Mono<ServerResponse> catalog(ServerRequest request) {
        return ok().syncBody(catalog);
    }

    Mono<ServerResponse> provisioning(ServerRequest request) {
        String instanceId = request.pathVariable("instanceId");
        log.info("Provisioning instanceId={}", instanceId);
        if (this.stateMap.containsKey(instanceId)) {
            ProvisioningState state = this.stateMap.get(instanceId);
            return status(HttpStatus.CONFLICT)
                    .syncBody(this.objectMapper.createObjectNode()
                            .put("description", instanceId + " already exists (State=" + state + ")"));
        }
        return request.bodyToMono(JsonNode.class)
                .filter(this::validateMandatoryInBody)
                .filter(this::validateGuidInBody)
                .flatMap(r -> {
                    this.stateMap.put(instanceId, IN_PROGRESS);
                    Mono<Void> provision = this.provisioner.provision(instanceId, namespace);
                    provision
                            .doOnSuccess(x -> this.stateMap.put(instanceId, SUCCEEDED))
                            .doOnError(e -> {
                                log.error("Provisioning failed.", e);
                                this.stateMap.put(instanceId, FAILED);
                            })
                            .subscribeOn(Schedulers.elastic())
                            .subscribe();
                    return status(HttpStatus.ACCEPTED)
                            .syncBody(this.objectMapper.createObjectNode());
                })
                .switchIfEmpty(this.badRequest);
    }

    Mono<ServerResponse> update(ServerRequest request) {
        String instanceId = request.pathVariable("instanceId");
        log.info("Updating instanceId={}", instanceId);
        return request.bodyToMono(JsonNode.class)
                .filter(this::validateMandatoryInBody)
                .flatMap(r -> ok().syncBody(this.objectMapper.createObjectNode()))
                .switchIfEmpty(this.badRequest);
    }

    Mono<ServerResponse> deprovisioning(ServerRequest request) {
        String instanceId = request.pathVariable("instanceId");
        log.info("Deprovisioning instanceId={}", instanceId);
        if (!this.validateParameters(request)) {
            return this.badRequest;
        }
        if (!this.stateMap.containsKey(instanceId)) {
            return status(HttpStatus.GONE)
                    .syncBody(this.objectMapper.createObjectNode()
                            .put("description", instanceId + " does not exist."));
        }
        return this.provisioner.deprovision(instanceId, namespace)
                .subscribeOn(Schedulers.elastic())
                .then(ok().syncBody(this.objectMapper.createObjectNode()))
                .doOnSuccess(x -> this.stateMap.remove(instanceId));
    }

    Mono<ServerResponse> lastOperation(ServerRequest request) {
        String instanceId = request.pathVariable("instanceId");
        log.info("LastOperation instanceId={}", instanceId);
        ProvisioningState state = this.stateMap.get(instanceId);
        if (state == null) {
            return status(HttpStatus.GONE)
                    .syncBody(this.objectMapper.createObjectNode()
                            .put("description", instanceId + " does not exist."));
        }
        if (state == IN_PROGRESS) {
            this.provisioner.isPodRunning(instanceId, namespace)
                    .subscribeOn(Schedulers.elastic())
                    .doOnNext(running -> {
                        if (running) {
                            this.stateMap.put(instanceId, SUCCEEDED);
                        }
                    }).subscribe();
        }
        return ok().syncBody(this.objectMapper.createObjectNode()
                .put("state", state.toString()));
    }

    Mono<ServerResponse> bind(ServerRequest request) {
        String instanceId = request.pathVariable("instanceId");
        String bindingId = request.pathVariable("bindingId");
        log.info("bind instanceId={}, bindingId={}", instanceId, bindingId);
        return request.bodyToMono(JsonNode.class) //
                .filter(this::validateMandatoryInBody) //
                .flatMap(r -> this.provisioner
                        .createUser(instanceId, bindingId, namespace)
                        .subscribeOn(Schedulers.elastic())
                        .map(credentials -> singletonMap("credentials", credentials))
                        .flatMap(body -> status(HttpStatus.CREATED).syncBody(body)))
                .switchIfEmpty(this.badRequest)
                .onErrorResume(e -> e instanceof ConflictException,
                        e -> status(HttpStatus.CONFLICT)
                                .syncBody(this.objectMapper.createObjectNode()
                                        .put("description", e.getMessage())));
    }

    Mono<ServerResponse> unbind(ServerRequest request) {
        String instanceId = request.pathVariable("instanceId");
        String bindingId = request.pathVariable("bindingId");
        log.info("unbind instanceId={}, bindingId={}", instanceId, bindingId);
        if (!this.validateParameters(request)) {
            return this.badRequest;
        }
        return this.provisioner
                .dropUser(instanceId, bindingId, namespace)
                .subscribeOn(Schedulers.elastic())
                .then(ok().syncBody(this.objectMapper.createObjectNode()))
                .onErrorResume(e -> e instanceof ConflictException,
                        e -> status(HttpStatus.CONFLICT)
                                .syncBody(this.objectMapper.createObjectNode()
                                        .put("description", e.getMessage())));
    }

    private boolean validateParameters(ServerRequest request) {
        return request.queryParam("plan_id").isPresent()
                && request.queryParam("service_id").isPresent();
    }

    private boolean validateMandatoryInBody(JsonNode node) {
        return node.has("plan_id") && node.get("plan_id").asText().length() == 36 // TODO
                && node.has("service_id") && node.get("service_id").asText().length() == 36; // TODO
    }

    private boolean validateGuidInBody(JsonNode node) {
        return node.has("organization_guid") && node.has("space_guid");
    }

    private Mono<ServerResponse> basicAuthentication(ServerRequest request,
                                                     HandlerFunction<ServerResponse> function) {
        if (request.path().startsWith("/v2/")) {
            List<String> authorizations = request.headers().header(HttpHeaders.AUTHORIZATION);
            String username = System.getenv("SECURITY_USER_NAME");
            String password = System.getenv("SECURITY_USER_PASSWORD");
            String basic = Base64.getEncoder().encodeToString((username + ":" + password).getBytes());
            if (authorizations.isEmpty()
                    || authorizations.get(0).length() <= "Basic ".length()
                    || !authorizations.get(0).substring("Basic ".length()).equals(basic)) {
                return status(HttpStatus.UNAUTHORIZED)
                        .syncBody(this.objectMapper.createObjectNode() //
                                .put("description", "Unauthorized."));
            }
        }
        return function.handle(request);
    }

    private Mono<ServerResponse> versionCheck(ServerRequest request,
                                              HandlerFunction<ServerResponse> function) {
        List<String> apiVersion = request.headers().header("X-Broker-API-Version");
        if (CollectionUtils.isEmpty(apiVersion)) {
            return status(HttpStatus.PRECONDITION_FAILED)
                    .syncBody(this.objectMapper.createObjectNode() //
                            .put("description", "X-Broker-API-Version header is missing."));
        }
        return function.handle(request);
    }

    enum ProvisioningState {
        IN_PROGRESS, SUCCEEDED, FAILED;

        @Override
        public String toString() {
            return name().toLowerCase().replace("_", " ");
        }
    }
}