package org.phoebus.olog;

import java.io.IOException;
import java.util.List;
import java.util.ServiceLoader;
import java.util.logging.Logger;

import org.phoebus.olog.entity.Logbook;
import org.phoebus.olog.entity.Property;
import org.phoebus.olog.entity.Tag;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = { "org.phoebus.olog" })
public class OlogDataMigrationApplication implements CommandLineRunner
{
    static final Logger logger = Logger.getLogger("Olog-Migration");

    private static ServiceLoader<LogRetrieval> loader;

    public static void main(String[] args) throws IOException
    {
        logger.info("Starting Olog Service");
        ConfigurableApplicationContext olog = SpringApplication.run(OlogDataMigrationApplication.class, args);
        System.out.println("starting olog migration");
        System.out.println("started olog migration");
    }

    @Bean
    public OlogMigrationService getOlogMigrationService()
    {
        return new OlogMigrationService();
    }

    @Override
    public void run(String... args) throws Exception
    {
        OlogMigrationService service = getOlogMigrationService();
        service.status();

        // Find the Log Retrieval implementations
        loader = ServiceLoader.load(LogRetrieval.class);
        loader.stream().forEach(logRetrieval -> {
            List<Tag> tags = logRetrieval.get().retrieveTags();
            List<Tag> transferredTags = service.transferTags(tags);
            logger.info("Completed transfer for " + transferredTags.size() + " tags");

            List<Logbook> logbooks = logRetrieval.get().retrieveLogbooks();
            List<Logbook> transferredLogbooks = service.transferLogbooks(logbooks);
            logger.info("Completed transfer for " + transferredLogbooks.size() + " logbooks");

            List<Property> properties = logRetrieval.get().retrieveProperties();
            List<Property> transferredProperties = service.transferProperties(properties);
            logger.info("Completed transfer for " + transferredProperties.size() + " properties");

        });

    }

}