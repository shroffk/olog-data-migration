package org.phoebus.olog;

import static org.phoebus.olog.OlogDataMigrationApplication.logger;

import java.io.IOException;
import java.util.List;
import java.util.logging.Level;

import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.phoebus.olog.entity.Logbook;
import org.phoebus.olog.entity.Property;
import org.phoebus.olog.entity.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class OlogMigrationService
{

    @Autowired
    SequenceGenerator generator;

    // Read the elatic index and type from the application.properties
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


    @Autowired
    @Qualifier("indexClient")
    RestHighLevelClient client;


    public void status()
    {
        logger.info("Starting the olog migration process with " + this);
        try
        {
            logger.info("elastic client created: " + client.info(RequestOptions.DEFAULT).toString());
        } catch (IOException e)
        {
            logger.log(Level.SEVERE, "Failed to create elastic client ", e);
        }
    }

    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Transfer ( create ) a set of tags in the new Phoebus Olog service
     * 
     * @param tags
     * @return list of tags successfully transferred/created
     */
    public List<Tag> transferTags(List<Tag> tags)
    {

        BulkRequest bulk = new BulkRequest();
        tags.forEach(tag -> {
            try
            {
                bulk.add(new IndexRequest(ES_TAG_INDEX, ES_TAG_TYPE, tag.getName())
                        .source(mapper.writeValueAsBytes(tag), XContentType.JSON));
            } catch (JsonProcessingException e)
            {
                logger.log(Level.SEVERE, e.getMessage(), e);
            }
        });
        BulkResponse bulkResponse;
        try
        {
            bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            bulkResponse = client.bulk(bulk, RequestOptions.DEFAULT);
            if (bulkResponse.hasFailures())
            {
                // process failures by iterating through each bulk response item
                bulkResponse.forEach(response -> {
                    if (response.getFailure() != null)
                    {
                        logger.log(Level.SEVERE, response.getFailureMessage(), response.getFailure().getCause());
                    }
                });
            } else
            {
                return tags;
            }
        } catch (IOException e)
        {
            logger.log(Level.SEVERE, e.getMessage(), e);
        }
        return null;
    }

    /**
     * Transfer ( create ) a set of logbooks in the new Phoebus Olog service
     * 
     * @param logbooks
     * @return list of logbooks successfully transferred/created
     */
    public List<Logbook> transferLogbooks(List<Logbook> logbooks)
    {

        BulkRequest bulk = new BulkRequest();
        logbooks.forEach(logbook -> {
            try
            {
                bulk.add(new IndexRequest(ES_LOGBOOK_INDEX, ES_LOGBOOK_TYPE, logbook.getName())
                        .source(mapper.writeValueAsBytes(logbook), XContentType.JSON));
            } catch (JsonProcessingException e)
            {
                logger.log(Level.SEVERE, e.getMessage(), e);

            }
        });
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        BulkResponse bulkResponse;
        try
        {
            bulkResponse = client.bulk(bulk, RequestOptions.DEFAULT);
            if (bulkResponse.hasFailures())
            {
                // process failures by iterating through each bulk response item
                bulkResponse.forEach(response -> {
                    if (response.getFailure() != null)
                    {
                        logger.log(Level.SEVERE, response.getFailureMessage(), response.getFailure().getCause());
                    }
                });
            } else
            {
                return logbooks;
            }
        } catch (IOException e)
        {
            logger.log(Level.SEVERE, e.getMessage(), e);
        }
        return null;
    }

    /**
     * Transfer ( create ) a set of properties in the new Phoebus Olog service
     * 
     * @param properties
     * @return list of properties successfully transferred/created
     */
    public List<Property> transferProperties(List<Property> properties)
    {

        BulkRequest bulk = new BulkRequest();
        properties.forEach(property -> {
            try
            {
                bulk.add(new IndexRequest(ES_PROPERTY_INDEX, ES_PROPERTY_TYPE, property.getName())
                        .source(mapper.writeValueAsBytes(property), XContentType.JSON));
            } catch (JsonProcessingException e)
            {
                logger.log(Level.SEVERE, e.getMessage(), e);
            }
        });
        BulkResponse bulkResponse;
        try
        {
            bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            bulkResponse = client.bulk(bulk, RequestOptions.DEFAULT);

            if (bulkResponse.hasFailures())
            {
                // process failures by iterating through each bulk response item
                bulkResponse.forEach(response -> {
                    if (response.getFailure() != null)
                    {
                        logger.log(Level.SEVERE, response.getFailureMessage(), response.getFailure().getCause());
                    }
                });
            } else
            {
                return properties;
            }
        } catch (IOException e)
        {
            logger.log(Level.SEVERE, e.getMessage(), e);
        }
        return null;
    }

    public void transferLogs()
    {

    }
}