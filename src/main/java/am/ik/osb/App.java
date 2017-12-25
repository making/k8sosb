package am.ik.osb;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.http.server.reactive.HttpHandler;
import org.springframework.http.server.reactive.ReactorHttpHandlerAdapter;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.ipc.netty.http.server.HttpServer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.springframework.web.reactive.function.server.RequestPredicates.DELETE;
import static org.springframework.web.reactive.function.server.RequestPredicates.GET;
import static org.springframework.web.reactive.function.server.RouterFunctions.route;
import static org.springframework.web.reactive.function.server.ServerResponse.noContent;
import static org.springframework.web.reactive.function.server.ServerResponse.ok;

public class App {

    static RouterFunction<ServerResponse> routes() {
        Config config = new ConfigBuilder().build();
        DefaultKubernetesClient kubernetesClient = new DefaultKubernetesClient(config);
        PostgresProvisioner provisioner = new PostgresProvisioner(kubernetesClient);
        String namespace = System.getenv("SERVICE_INSTANCE_NS");
        Resource catalog = new ClassPathResource("catalog.yml");
        try {
            return new ServiceBrokerHandler(provisioner, namespace, catalog).routes();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();

        long begin = System.currentTimeMillis();
        int port = Optional.ofNullable(System.getenv("PORT")) //
                .map(Integer::parseInt) //
                .orElse(8080);
        HttpServer httpServer = HttpServer.create("0.0.0.0", port);
        httpServer.startRouterAndAwait(routes -> {
            HttpHandler httpHandler = RouterFunctions.toHttpHandler(App.routes());
            routes.route(x -> true, new ReactorHttpHandlerAdapter(httpHandler));
        }, context -> {
            long elapsed = System.currentTimeMillis() - begin;
            LoggerFactory.getLogger(App.class).info("Started in {} seconds", elapsed / 1000.0);
        });
    }
}
