package org.phoebus.olog;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.GetRequest;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.phoebus.olog.entity.Attachment;
import org.phoebus.olog.entity.Log;
import org.phoebus.olog.entity.Log.LogBuilder;
import org.phoebus.olog.entity.Logbook;
import org.phoebus.olog.entity.Property;
import org.phoebus.olog.entity.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.gridfs.GridFsOperations;
import org.springframework.data.mongodb.gridfs.GridFsTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;

import static org.phoebus.olog.OlogDataMigrationApplication.logger;

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
    @Qualifier("client")
    ElasticsearchClient client;

    public void status()
    {
        logger.info("Starting the olog migration process with " + this);
        try
        {
            logger.info("elastic client created: " + client.info().toString());
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

        List<BulkOperation> bulkOperations = new ArrayList<>();
        tags.forEach(tag -> bulkOperations.add(IndexOperation.of(i ->
                i.index(ES_TAG_INDEX).document(tag).id(tag.getName()))._toBulkOperation()));
        BulkRequest bulkRequest =
                BulkRequest.of(r ->
                        r.operations(bulkOperations).refresh(Refresh.True));

        BulkResponse bulkResponse;
        try {
            bulkResponse = client.bulk(bulkRequest);
            if (bulkResponse.errors()) {
                // process failures by iterating through each bulk response item
                bulkResponse.items().forEach(responseItem -> {
                    if (responseItem.error() != null) {
                        logger.log(Level.SEVERE, responseItem.error().reason());
                    }
                });
                String message = MessageFormat.format("Failed to create tags {0}", tags);
                throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, message);
            } else {
                return tags;
            }
        } catch (IOException e) {
            String message = MessageFormat.format("Failed to create tags {0}", tags);
            logger.log(Level.SEVERE, message, e);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, message);
        }
    }

    /**
     * Transfer ( create ) a set of logbooks in the new Phoebus Olog service
     * 
     * @param logbooks
     * @return list of logbooks successfully transferred/created
     */
    public List<Logbook> transferLogbooks(List<Logbook> logbooks)
    {
        List<BulkOperation> bulkOperations = new ArrayList<>();
        logbooks.forEach(logbook -> bulkOperations.add(IndexOperation.of(i ->
                i.index(ES_LOGBOOK_INDEX).document(logbook).id(logbook.getName()))._toBulkOperation()));
        BulkRequest bulkRequest =
                BulkRequest.of(r ->
                        r.operations(bulkOperations).refresh(Refresh.True));
        BulkResponse bulkResponse;
        try {
            bulkResponse = client.bulk(bulkRequest);
            if (bulkResponse.errors()) {
                // process failures by iterating through each bulk response item
                bulkResponse.items().forEach(responseItem -> {
                    if (responseItem.error() != null) {
                        logger.log(Level.SEVERE, responseItem.error().reason());
                    }
                });
                String message = MessageFormat.format("Failed to created logbooks {0}", logbooks);
                throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, message);
            } else {
                return logbooks;
            }
        } catch (IOException e) {
            String message = MessageFormat.format("Failed to created logbooks {0}", logbooks);
            logger.log(Level.SEVERE, message, e);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, message);
        }
    }

    /**
     * Transfer ( create ) a set of properties in the new Phoebus Olog service
     * 
     * @param properties
     * @return list of properties successfully transferred/created
     */
    public List<Property> transferProperties(List<Property> properties)
    {
        List<BulkOperation> bulkOperations = new ArrayList<>();
        properties.forEach(property -> bulkOperations.add(IndexOperation.of(i ->
                i.index(ES_PROPERTY_INDEX).document(property).id(property.getName()))._toBulkOperation()));

        BulkRequest bulkRequest = BulkRequest.of(r ->
                r.operations(bulkOperations).refresh(Refresh.True));

        BulkResponse bulkResponse;
        try {
            bulkResponse = client.bulk(bulkRequest);
            if (bulkResponse.errors()) {
                // process failures by iterating through each bulk response item
                bulkResponse.items().forEach(responseItem -> {
                    if (responseItem.error() != null) {
                        logger.log(Level.SEVERE, responseItem.error().reason());
                    }
                });
                String message = MessageFormat.format("Failed to create properties {0}", properties);
                throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, message);
            } else {

                return properties;
            }
        } catch (IOException e) {
            String message = MessageFormat.format("Failed to create properties {0}", properties);
            logger.log(Level.SEVERE, message, e);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, message);
        }
    }

    public List<Log> transferLogs(List<Log> logs)
    {

        ArrayList<Log> transferredLogs = new ArrayList<Log>();
        for (Log log : logs)
        {
            try
            {
                Long id = generator.getID();
                LogBuilder validatedLog = LogBuilder.createLog(log).id(id).createDate(log.getCreatedDate());
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

                Log document = validatedLog.build();

                IndexRequest<Object> indexRequest =
                        IndexRequest.of(i ->
                                i.index(ES_LOG_INDEX)
                                        .id(String.valueOf(id))
                                        .document(document)
                                        .refresh(Refresh.True));
                IndexResponse response = client.index(indexRequest);

                if (response.result().equals(Result.Created)) {
                    GetRequest getRequest =
                            GetRequest.of(g ->
                                    g.index(ES_LOG_INDEX).id(response.id()));
                    GetResponse<Log> resp =
                            client.get(getRequest, Log.class);
                    transferredLogs.add(resp.source());
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