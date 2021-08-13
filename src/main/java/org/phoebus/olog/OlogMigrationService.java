package org.phoebus.olog;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.phoebus.olog.entity.Logbook;
import org.phoebus.olog.entity.Property;
import org.phoebus.olog.entity.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;

import static org.phoebus.olog.OlogDataMigrationApplication.logger;

import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
public class OlogMigrationService {

    @Autowired
    SequenceGenerator generator;

    @Value("${elasticsearch.tag.index:olog_tags}")
    private String ES_TAG_INDEX;
    @Value("${elasticsearch.tag.type:olog_tag}")
    private String ES_TAG_TYPE;

    @Autowired
    @Qualifier("indexClient")
    RestHighLevelClient client;
    
    public void status() {
        logger.info("Starting the olog migration process with " + this);
        try
        {
            logger.info("elastic client created: " + client.info(RequestOptions.DEFAULT).toString());
        } catch (IOException e)
        {
            logger.log(Level.SEVERE, "Failed to create elastic client ", e);
        }
    }

    public void transferTags(List<Tag> list) {
        
    }
    

    public void transferLogbooks(List<Logbook> logbooks) {
        
    }
    

    public void transferProperties(List<Property> properties) {
        
    }
    
    public void transferLogs() {
        
    }
}