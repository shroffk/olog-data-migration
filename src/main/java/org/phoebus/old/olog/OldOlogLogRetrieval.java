package org.phoebus.old.olog;

import java.net.URI;
import java.util.List;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;

import org.phoebus.olog.LogRetrieval;
import org.phoebus.olog.entity.Log;
import org.phoebus.olog.entity.Logbook;
import org.phoebus.olog.entity.Property;
import org.phoebus.olog.entity.Tag;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.multipart.impl.MultiPartWriter;

public class OldOlogLogRetrieval implements LogRetrieval
{
    private final WebResource service;
    private URI ologURI;

    public OldOlogLogRetrieval()
    {
        this.ologURI = URI.create("http://localhost:7070");
        
        DefaultClientConfig clientConfig = new DefaultClientConfig();
        clientConfig.getClasses().add(MultiPartWriter.class);
        Client client = Client.create(clientConfig);
        client.setFollowRedirects(true);
        service = client.resource(UriBuilder.fromUri(ologURI).build());
    }
    
    @Override
    public List<Tag> retrieveTags()
    {
        XmlTags allXmlTags = service.path("Olog/resources/tags").accept(MediaType.APPLICATION_XML).get(XmlTags.class);
        return null;
    }

    @Override
    public List<Logbook> retrieveLogbooks()
    {
        return null;
    }

    @Override
    public List<Property> retrieveProperties()
    {
        return null;
    }

    @Override
    public List<Log> retrieveLogs(double size, double from)
    {
        return null;
    }

}
