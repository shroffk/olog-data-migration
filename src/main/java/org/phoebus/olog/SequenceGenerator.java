/**
 * 
 */
package org.phoebus.olog;

import static org.phoebus.olog.OlogDataMigrationApplication.logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.logging.Level;

import javax.annotation.PostConstruct;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
/**
 * @author Kunal Shroff
 *
 */
@Service
public class SequenceGenerator
{

    @Value("${elasticsearch.sequence.index:olog_sequence}")
    private String ES_SEQUENCE_INDEX;
    @Value("${elasticsearch.sequence.type:olog_sequence}")
    private String ES_SEQUENCE_TYPE;

    @Autowired
    @Qualifier("indexClient")
    RestHighLevelClient indexClient;

    private static RestHighLevelClient client;

    @PostConstruct
    public void init()
    {
        OlogDataMigrationApplication.logger.config("Initializing the unique sequence id generator");
        SequenceGenerator.client = indexClient;
    }

    /**
     * get a new unique id from the olog_sequnce index
     * 
     * @return a new unique id for a olog entry
     * @throws IOException 
     */
    public long getID() throws IOException
    {
        IndexResponse response = client.index(
                new IndexRequest(ES_SEQUENCE_INDEX, ES_SEQUENCE_TYPE, "id").source(0, XContentType.class),
                RequestOptions.DEFAULT);
        return response.getVersion();
    }

    public void resetID() throws IOException
    {
        // Create/migrate the sequence index
        try
        {
            AcknowledgedResponse response = indexClient.indices().delete(new DeleteIndexRequest(ES_SEQUENCE_INDEX), RequestOptions.DEFAULT);
            CreateIndexRequest createRequest = new CreateIndexRequest(ES_SEQUENCE_INDEX);
            createRequest
                    .settings(Settings.builder().put("index.number_of_shards", 1).put("auto_expand_replicas", "0-all"));
            ObjectMapper mapper = new ObjectMapper();
            InputStream is = ElasticConfig.class.getResourceAsStream("/seq_mapping.json");
            Map<String, String> jsonMap = mapper.readValue(is, Map.class);
            createRequest.mapping(ES_SEQUENCE_TYPE, jsonMap);
            logger.info("Successfully created index: " + ES_SEQUENCE_TYPE);
        } catch (IOException e)
        {
            logger.log(Level.WARNING, "Failed to create index " + ES_SEQUENCE_INDEX, e);
        }
    }

}
