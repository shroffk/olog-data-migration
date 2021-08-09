package org.phoebus.olog;

import java.io.IOException;
import java.util.logging.Logger;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = { "org.phoebus.olog" })
public class OlogDataMigrationApplication implements CommandLineRunner {
    static final Logger logger = Logger.getLogger("Olog-Migration");

    
    public static void main(String[] args) throws IOException
    {
        logger.info("Starting Olog Service");
        ConfigurableApplicationContext olog = SpringApplication.run(OlogDataMigrationApplication.class, args);
        System.out.println("starting olog migration");
        System.out.println("started olog migration");
    }

    @Bean
    public OlogMigrationService getOlogMigrationService(){
        return new OlogMigrationService();
    }

    @Override
    public void run(String... args) throws Exception {
        OlogMigrationService service = getOlogMigrationService();
        service.hello();
        service.update();

    }

}