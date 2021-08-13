package org.phoebus.old.olog;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriBuilder;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.phoebus.olog.LogRetrieval;
import org.phoebus.olog.entity.Attribute;
import org.phoebus.olog.entity.Log;
import org.phoebus.olog.entity.Log.LogBuilder;
import org.phoebus.olog.entity.Logbook;
import org.phoebus.olog.entity.Property;
import org.phoebus.olog.entity.State;
import org.phoebus.olog.entity.Tag;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.core.util.MultivaluedMapImpl;
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
        return allXmlTags.getTags().stream().map((xmlTag) -> {
            return new Tag(xmlTag.getName(),
                    xmlTag.getState().equalsIgnoreCase("Active") ? State.Active : State.Inactive);
        }).collect(Collectors.toList());
    }

    @Override
    public List<Logbook> retrieveLogbooks()
    {
        XmlLogbooks allXmlLogbooks = service.path("Olog/resources/logbooks").accept(MediaType.APPLICATION_XML)
                .get(XmlLogbooks.class);
        return allXmlLogbooks.getLogbooks().stream().map((xmlLogbook) -> {
            return new Logbook(xmlLogbook.getName(), xmlLogbook.getOwner());
        }).collect(Collectors.toList());
    }

    @Override
    public List<Property> retrieveProperties()
    {
        XmlProperties allXmlProperties = service.path("Olog/resources/properties").accept(MediaType.APPLICATION_XML)
                .get(XmlProperties.class);
        return allXmlProperties.getProperties().stream().map((xmlProperty) -> {
            Property property = new Property(xmlProperty.getName());
            property.addAttributes(xmlProperty.getAttributes().entrySet().stream().map((xmlAttribute) -> {
                return new Attribute(xmlAttribute.getKey());
            }).collect(Collectors.toSet()));
            return property;
        }).collect(Collectors.toList());
    }

    @Override
    public int retireveLogCount()
    {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.add("limit", "1");
        String response = service.path("Olog/resources/logs").queryParams(map).accept(MediaType.APPLICATION_XML)
                .get(String.class);
        try
        {
            DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
            Document document = docBuilder.parse(new ByteArrayInputStream(response.getBytes()));
            Node logs = document.getFirstChild();
            NamedNodeMap attributes = logs.getAttributes();
            return Integer.valueOf(attributes.getNamedItem("count").getNodeValue());
        } catch (ParserConfigurationException | SAXException | IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public List<Log> retrieveLogs(double size, double page)
    {

        // Since olog return the log in reverse order in order to get them in the order
        // of ascending
        // id's we need to reverse map them.
        int count = retireveLogCount();
        double mappedPage = Math.ceil(count / size) + 1 - page;

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.add("limit", String.valueOf(size));
        map.add("page", String.valueOf(mappedPage));
        XmlLogs xmlLogs = service.path("logs").queryParams(map).accept(MediaType.APPLICATION_XML)
                .accept(MediaType.APPLICATION_JSON).get(XmlLogs.class);

        List<Log> logs = new ArrayList<Log>();
        logs = xmlLogs.getLogs().stream().map((xmlLog) -> {
            LogBuilder log = Log.LogBuilder.createLog(xmlLog.getDescription());
            if (xmlLog.getLevel() != null && !xmlLog.getLevel().isBlank())
                log.level(xmlLog.getLevel());
            if (xmlLog.getCreatedDate() != null)
                log.createDate(xmlLog.getCreatedDate().toInstant());

            // map the logbooks
            if (xmlLog.getXmlLogbooks() != null && !xmlLog.getXmlLogbooks().isEmpty())
                log.setLogbooks(xmlLog.getXmlLogbooks().stream().map((xmlLogbook) -> {
                    return new Logbook(xmlLogbook.getName(), xmlLogbook.getOwner());
                }).collect(Collectors.toSet()));

            // map the tags
            if (xmlLog.getXmlTags() != null && !xmlLog.getXmlTags().isEmpty())
                log.setTags(xmlLog.getXmlTags().stream().map((xmlTag) -> {
                    return new Tag(xmlTag.getName(),
                            xmlTag.getState().equalsIgnoreCase("Active") ? State.Active : State.Inactive);
                }).collect(Collectors.toSet()));

            // map the properties
            if (xmlLog.getXmlProperties() != null && !xmlLog.getXmlProperties().isEmpty())
                log.setProperties(xmlLog.getXmlProperties().stream().map((xmlProperty) -> {
                    Property property = new Property(xmlProperty.getName());
                    property.addAttributes(xmlProperty.getAttributes().entrySet().stream().map((xmlAttribute) -> {
                        return new Attribute(xmlAttribute.getKey(), xmlAttribute.getValue());
                    }).collect(Collectors.toSet()));
                    return property;
                }).collect(Collectors.toSet()));
            
            return log.build();
        })
                .sorted(Comparator.comparingLong(Log::getId))
                .collect(Collectors.toList());

        return logs;
    }

}
