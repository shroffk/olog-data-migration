/**
 *
 */
package org.phoebus.olog;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.AcknowledgedResponse;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;
import java.text.MessageFormat;
import java.time.Instant;
import java.util.logging.Level;

import static org.phoebus.olog.ElasticConfig.ELASTIC_CREATED_INDEX_ACKNOWLEDGED;
import static org.phoebus.olog.ElasticConfig.ELASTIC_FAILED_TO_CREATE_INDEX;
import static org.phoebus.olog.OlogDataMigrationApplication.logger;

/**
 * @author Kunal Shroff
 *
 */
@Service
public class SequenceGenerator {

    @Value("${elasticsearch.sequence.index:olog_sequence}")
    private String ES_SEQUENCE_INDEX;
    @Value("${elasticsearch.sequence.type:olog_sequence}")
    private String ES_SEQUENCE_TYPE;

    @Autowired
    @Qualifier("client")
    private ElasticsearchClient client;

    private OlogSequence seq;
    private ObjectMapper objectMapper;
    private IndexRequest request;

    @PostConstruct
    public void init() {
        OlogDataMigrationApplication.logger.config("Initializing the unique sequence id generator");
        seq = new OlogSequence();
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        request = IndexRequest.of(i -> i.index(ES_SEQUENCE_INDEX)
                .document(JsonData.of(seq, new JacksonJsonpMapper(objectMapper)))
                .refresh(Refresh.True));
    }

    /**
     * get a new unique id from the olog_sequnce index
     *
     * @return a new unique id for a olog entry
     * @throws IOException
     */
    public long getID() throws IOException {

        return client.index(request).seqNo();
    }

    public void resetID() throws IOException {
        // Create/migrate the sequence index
        try {
            AcknowledgedResponse response = client.indices().delete(DeleteIndexRequest.of(d -> d.index(ES_SEQUENCE_INDEX)));
            try (InputStream is = ElasticConfig.class.getResourceAsStream("/seq_mapping.json")) {
                BooleanResponse exists = client.indices().exists(ExistsRequest.of(e -> e.index(ES_SEQUENCE_INDEX)));
                if (!exists.value()) {
                    CreateIndexRequest request = CreateIndexRequest.of(
                            c -> (c).index(ES_SEQUENCE_INDEX)
                                    .withJson(is)
                    );
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
                    CreateIndexResponse result = client.indices().create(
                            request
                    );
                    logger.log(Level.INFO, () -> MessageFormat.format(ELASTIC_CREATED_INDEX_ACKNOWLEDGED, ES_SEQUENCE_INDEX, result.acknowledged()));
                }
            } catch (IOException e) {
                logger.log(Level.WARNING, MessageFormat.format(ELASTIC_FAILED_TO_CREATE_INDEX, ES_SEQUENCE_INDEX), e);
            }
        } finally {

        }
    }

    private static class OlogSequence {
        private final Instant createDate;

        OlogSequence() {
            createDate = Instant.now();
        }

        public Instant getCreateDate() {
            return createDate;
        }
    }
}
