package org.phoebus.olog;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;

public class OlogMigrationService {

    @Autowired
    SequenceGenerator generator;
    
    public void hello() {
       System.out.println("Hello from Hello Service");
    }
    
    public void update() throws IOException {
        System.out.println(generator.getID());
        System.out.println(generator.getID());
        generator.resetID();
        System.out.println(generator.getID());
        System.out.println(generator.getID());
        
    }
    
    public void transferTags() {
        
    }
}