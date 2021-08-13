package org.phoebus.old.olog;

import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;

import org.phoebus.olog.LogRetrieval;
import org.phoebus.olog.entity.Attribute;
import org.phoebus.olog.entity.Log;
import org.phoebus.olog.entity.Logbook;
import org.phoebus.olog.entity.Property;
import org.phoebus.olog.entity.State;
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
        return allXmlTags.getTags().stream().map((xmlTag)->{
            return new Tag(xmlTag.getName(), xmlTag.getState().equalsIgnoreCase("Active")? State.Active: State.Inactive);
        }).collect(Collectors.toList());
    }

    @Override
    public List<Logbook> retrieveLogbooks()
    {
        XmlLogbooks allXmlLogbooks = service.path("Olog/resources/logbooks").accept(MediaType.APPLICATION_XML).get(XmlLogbooks.class);
        return allXmlLogbooks.getLogbooks().stream().map((xmlLogbook)->{
            return new Logbook(xmlLogbook.getName(), xmlLogbook.getOwner());
        }).collect(Collectors.toList());
    }

    @Override
    public List<Property> retrieveProperties()
    {
        XmlProperties allXmlProperties = service.path("Olog/resources/properties").accept(MediaType.APPLICATION_XML).get(XmlProperties.class);
        return allXmlProperties.getProperties().stream().map((xmlProperty) -> {
            Property property = new Property(xmlProperty.getName());
            property.addAttributes(xmlProperty.getAttributes().entrySet().stream().map((xmlAttribute) -> {
                return new Attribute(xmlAttribute.getKey());
            }).collect(Collectors.toSet()));
            return property;
        } ).collect(Collectors.toList());
    }

    @Override
    public List<Log> retrieveLogs(double size, double from)
    {
        return null;
    }

}
