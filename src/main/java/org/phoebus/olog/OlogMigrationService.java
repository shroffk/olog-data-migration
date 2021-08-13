package org.phoebus.olog;

import static org.phoebus.olog.OlogDataMigrationApplication.logger;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.http.HttpHost;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentType;
import org.phoebus.olog.entity.Attachment;
import org.phoebus.olog.entity.Log;
import org.phoebus.olog.entity.Logbook;
import org.phoebus.olog.entity.Property;
import org.phoebus.olog.entity.Tag;
import org.phoebus.olog.entity.Log.LogBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.gridfs.GridFsOperations;
import org.springframework.data.mongodb.gridfs.GridFsTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;

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
    @Value("${elasticsearch.log.index:olog_logs}")
    private String ES_LOG_INDEX;
    @Value("${elasticsearch.log.type:olog_log}")
    private String ES_LOG_TYPE;

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

    public List<Log> transferLogs(List<Log> logs)
    {

        ArrayList<Log> transferredLogs = new ArrayList<Log>();
        for (Log log : logs)
        {
            try
            {
                Long id = generator.getID();
                LogBuilder validatedLog = LogBuilder.createLog(log).id(id).createDate(Instant.now());
                if (log.getAttachments() != null && !log.getAttachments().isEmpty())
                {
                    Set<Attachment> createdAttachments = new HashSet<Attachment>();
                    log.getAttachments().stream().filter(attachment -> {
                        return attachment.getAttachment() != null;
                    }).forEach(attachment -> {
                        createdAttachments.add(saveAttachment(attachment));
                    });
                    validatedLog = validatedLog.setAttachments(createdAttachments);
                }

                IndexRequest indexRequest = new IndexRequest(ES_LOG_INDEX, ES_LOG_TYPE, String.valueOf(id))
                        .source(mapper.writeValueAsBytes(validatedLog.build()), XContentType.JSON)
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

                IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);

                if (response.getResult().equals(Result.CREATED))
                {
                    BytesReference ref = client
                            .get(new GetRequest(ES_LOG_INDEX, ES_LOG_TYPE, response.getId()), RequestOptions.DEFAULT)
                            .getSourceAsBytesRef();

                    Log createdLog = mapper.readValue(ref.streamInput(), Log.class);
                    transferredLogs.add(createdLog);
                }
            } catch (Exception e)
            {
                logger.log(Level.SEVERE, e.getMessage(), e);
            }
        }
        return transferredLogs;
    }
    
    @Autowired
    private GridFsTemplate gridFsTemplate;
    @Autowired
    private GridFsOperations gridOperation;
    @Autowired
    private GridFSBucket gridFSBucket;

    public Attachment saveAttachment(Attachment entity)
    {
        try
        {
            GridFSUploadOptions options = new GridFSUploadOptions()
                    .metadata(new Document("meta-data", entity.getFileMetadataDescription()));
            if(entity.getId() != null && !entity.getId().isEmpty()){
                BsonString id = new BsonString(entity.getId());
                gridFSBucket.uploadFromStream(id, entity.getFilename(), entity.getAttachment().getInputStream(), options);
            }
            else{
                ObjectId objectId = gridFSBucket.uploadFromStream(entity.getFilename(), entity.getAttachment().getInputStream(), options);
                entity.setId(objectId.toString());
            }
            return entity;
        } catch (IOException e)
        {
            logger.log(Level.WARNING, String.format("Unable to persist attachment %s", entity.getFilename()), e);
        }
        return null;
    }
}