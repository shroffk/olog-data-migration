package org.phoebus.olog;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import java.io.IOException;
import java.io.InputStream;
import java.text.MessageFormat;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A element which creates the elastic rest clients used the olog service for
 * creating and retrieving logs and other resources
 * 
 * @author kunal
 *
 */
@Configuration
@ComponentScan(basePackages = { "org.phoebus.olog" })
@PropertySource("classpath:application.properties")
public class ElasticConfig
{
    public static final String ELASTIC_CREATED_INDEX_ACKNOWLEDGED       = "Created index {0} acknowledged {1}";
    public static final String ELASTIC_FAILED_TO_CREATE_INDEX           = "Failed to create index {0}";

    private static final Logger logger = Logger.getLogger(ElasticConfig.class.getName());

    // Read the elatic index and type from the application.properties
    @Value("${elasticsearch.http.connect_timeout_ms:" + RestClientBuilder.DEFAULT_CONNECT_TIMEOUT_MILLIS + "}") // default 1 second
    private Integer ES_HTTP_CONNECT_TIMEOUT_MS;
    @Value("${elasticsearch.http.socket_timeout_ms:" + RestClientBuilder.DEFAULT_SOCKET_TIMEOUT_MILLIS + "}") // default 30 seconds
    private Integer ES_HTTP_SOCKET_TIMEOUT_MS;
    @Value("${elasticsearch.http.keep_alive_timeout_ms:30000}") // default 30 seconds
    private Long ES_HTTP_CLIENT_KEEP_ALIVE_TIMEOUT_MS;
    @Value("${elasticsearch.index.create.timeout:30s}")
    private String ES_INDEX_CREATE_TIMEOUT;
    @Value("${elasticsearch.index.create.master_timeout:30s}")
    private String ES_INDEX_CREATE_MASTER_TIMEOUT;
    @Value("${elasticsearch.tag.index:olog_tags}")
    private String ES_TAG_INDEX;
    @Value("${elasticsearch.tag.type:olog_tag}")
    private String ES_TAG_TYPE;
    @Value("${elasticsearch.logbook.index:olog_logbooks}")
    private String ES_LOGBOOK_INDEX;
    @Value("${elasticsearch.logbook.type:olog_logbook}")
    private String ES_LOGBOOK_TYPE;
    @Value("${elasticsearch.property.index:olog_properties}")
    private String ES_PROPERTY_INDEX;
    @Value("${elasticsearch.property.type:olog_property}")
    private String ES_PROPERTY_TYPE;
    @Value("${elasticsearch.log.index:olog_logs}")
    private String ES_LOG_INDEX;
    @Value("${elasticsearch.log.type:olog_log}")
    private String ES_LOG_TYPE;
    @Value("${elasticsearch.sequence.index:olog_sequence}")
    private String ES_SEQ_INDEX;
    @Value("${elasticsearch.sequence.type:olog_sequence}")
    private String ES_SEQ_TYPE;

    @Value("${elasticsearch.cluster.name:elasticsearch}")
    private String clusterName;
    @Value("${elasticsearch.network.host:localhost}")
    private String host;
    @Value("${elasticsearch.http.port:9200}")
    private int port;
    @Value("${elasticsearch.http.protocol:http}")
    private String protocol;
    @Value("${elasticsearch.create.indices:true}")
    private String createIndices;

    private ElasticsearchClient client;
    private static final AtomicBoolean esInitialized = new AtomicBoolean();

    private CreateIndexRequest.Builder withTimeouts(CreateIndexRequest.Builder builder) {
        return builder
                .timeout(timeBuilder ->
                        timeBuilder.time(ES_INDEX_CREATE_TIMEOUT)
                ).masterTimeout( timeBuilder ->
                        timeBuilder.time(ES_INDEX_CREATE_MASTER_TIMEOUT)
                );
    }

    private void logCreateIndexRequest(CreateIndexRequest request) {
        logger.log(Level.INFO, () -> String.format(
                "CreateIndexRequest: " +
                        "index: %s, " +
                        "timeout: %s, " +
                        "masterTimeout: %s, " +
                        "waitForActiveShards: %s",
                request.index(),
                request.timeout() != null ? request.timeout().time() : null,
                request.masterTimeout() != null ? request.masterTimeout().time() : null,
                request.waitForActiveShards() != null ? request.waitForActiveShards()._toJsonString() : null
        ));
    }

    @Bean({"client"})
    public ElasticsearchClient getClient() {
        if (client == null) {
            // Create the low-level client
            logger.log(Level.INFO, () -> String.format("Creating HTTP client with " +
                            "host %s, " +
                            "port %s, " +
                            "protocol %s, " +
                            "keep-alive %s ms, " +
                            "connect timeout %s ms, " +
                            "socket timeout %s ms",
                    host, port, protocol,
                    ES_HTTP_CLIENT_KEEP_ALIVE_TIMEOUT_MS,
                    ES_HTTP_CONNECT_TIMEOUT_MS,
                    ES_HTTP_SOCKET_TIMEOUT_MS
            ));
            RestClient httpClient = null;
            try {
                httpClient = RestClient.builder(new HttpHost(host, port))
                        .setRequestConfigCallback( builder ->
                                builder.setConnectTimeout(ES_HTTP_CONNECT_TIMEOUT_MS)
                                        .setSocketTimeout(ES_HTTP_SOCKET_TIMEOUT_MS)
                        )
                        .setHttpClientConfigCallback(builder ->
                                // Avoid timeout problems
                                // https://github.com/elastic/elasticsearch/issues/65213
                                builder.setKeepAliveStrategy((response, context) -> ES_HTTP_CLIENT_KEEP_ALIVE_TIMEOUT_MS)
                        )
                        .build();
            } catch (Exception e) {
                logger.log(Level.WARNING, e.getMessage(), e);
            }


            // Create the Java API Client with the same low level client
            ElasticsearchTransport transport = new RestClientTransport(
                    httpClient,
                    new JacksonJsonpMapper()
            );
            client = new ElasticsearchClient(transport);
            esInitialized.set(!Boolean.parseBoolean(createIndices));
            if (esInitialized.compareAndSet(false, true)) {
                elasticIndexValidation(client);
            }
        }
        return client;
    }

    /**
     * Checks for the existence of the elastic indices needed for Olog and creates
     * them with the appropriate mapping is they are missing.
     * 
     * @param client the elastic client instance used to validate and create
     *                    olog indices
     */
    @SuppressWarnings({ "deprecation", "unchecked" })
    private synchronized void elasticIndexValidation(ElasticsearchClient client)
    {

        // Olog Sequence Index
        try (InputStream is = ElasticConfig.class.getResourceAsStream("/seq_mapping.json")) {
            BooleanResponse exists = client.indices().exists(ExistsRequest.of(e -> e.index(ES_SEQ_INDEX)));
            if(!exists.value()) {
                CreateIndexRequest request = CreateIndexRequest.of(
                        c -> withTimeouts(c).index(ES_SEQ_INDEX)
                                .withJson(is)
                );
                logCreateIndexRequest(request);
                CreateIndexResponse result = client.indices().create(
                        request
                );
                logger.log(Level.INFO, () -> MessageFormat.format(ELASTIC_CREATED_INDEX_ACKNOWLEDGED, ES_SEQ_INDEX, result.acknowledged()));
            }
        } catch (IOException e) {
            logger.log(Level.WARNING, MessageFormat.format(ELASTIC_FAILED_TO_CREATE_INDEX, ES_SEQ_INDEX), e);
        }

        // Olog Logbook Index
        try (InputStream is = ElasticConfig.class.getResourceAsStream("/logbook_mapping.json")) {
            BooleanResponse exits = client.indices().exists(ExistsRequest.of(e -> e.index(ES_LOGBOOK_INDEX)));
            if(!exits.value()) {
                CreateIndexRequest request = CreateIndexRequest.of(
                        c -> withTimeouts(c).index(ES_LOGBOOK_INDEX).withJson(is)
                );
                logCreateIndexRequest(request);
                CreateIndexResponse result = client.indices().create(request);
                logger.log(Level.INFO, () -> MessageFormat.format(ELASTIC_CREATED_INDEX_ACKNOWLEDGED, ES_LOGBOOK_INDEX, result.acknowledged()));
            }
        } catch (IOException e) {
            logger.log(Level.WARNING, MessageFormat.format(ELASTIC_FAILED_TO_CREATE_INDEX, ES_LOGBOOK_INDEX), e);
        }

        // Olog Tag Index
        try (InputStream is = ElasticConfig.class.getResourceAsStream("/tag_mapping.json")) {
            BooleanResponse exits = client.indices().exists(ExistsRequest.of(e -> e.index(ES_TAG_INDEX)));
            if(!exits.value()) {
                CreateIndexRequest request = CreateIndexRequest.of(
                        c -> withTimeouts(c).index(ES_TAG_INDEX).withJson(is)
                );
                logCreateIndexRequest(request);
                CreateIndexResponse result = client.indices().create(request);
                logger.log(Level.INFO, () -> MessageFormat.format(ELASTIC_CREATED_INDEX_ACKNOWLEDGED, ES_TAG_INDEX, result.acknowledged()));
            }
        } catch (IOException e) {
            logger.log(Level.WARNING, MessageFormat.format(ELASTIC_FAILED_TO_CREATE_INDEX, ES_TAG_INDEX), e);
        }

        // Olog Property Index
        try (InputStream is = ElasticConfig.class.getResourceAsStream("/property_mapping.json")) {
            BooleanResponse exits = client.indices().exists(ExistsRequest.of(e -> e.index(ES_PROPERTY_INDEX)));
            if(!exits.value()) {
                CreateIndexRequest request = CreateIndexRequest.of(
                        c -> withTimeouts(c).index(ES_PROPERTY_INDEX).withJson(is)
                );
                logCreateIndexRequest(request);
                CreateIndexResponse result = client.indices().create(request);
                logger.log(Level.INFO, () -> MessageFormat.format(ELASTIC_CREATED_INDEX_ACKNOWLEDGED, ES_PROPERTY_INDEX, result.acknowledged()));
            }
        } catch (IOException e) {
            logger.log(Level.WARNING, MessageFormat.format(ELASTIC_FAILED_TO_CREATE_INDEX, ES_PROPERTY_INDEX), e);
        }

        // Olog Log Template
        try (InputStream is = ElasticConfig.class.getResourceAsStream("/log_entry_mapping.json")) {
            BooleanResponse exits = client.indices().exists(ExistsRequest.of(e -> e.index(ES_LOG_INDEX)));
            if(!exits.value()) {
                CreateIndexRequest request = CreateIndexRequest.of(
                        c -> withTimeouts(c).index(ES_LOG_INDEX).withJson(is)
                );
                logCreateIndexRequest(request);
                CreateIndexResponse result = client.indices().create(request);
                logger.log(Level.INFO, () -> MessageFormat.format(ELASTIC_CREATED_INDEX_ACKNOWLEDGED, ES_LOG_INDEX, result.acknowledged()));
            }
        } catch (IOException e) {
            logger.log(Level.WARNING, MessageFormat.format(ELASTIC_FAILED_TO_CREATE_INDEX, ES_LOG_INDEX), e);
        }
    }

}
