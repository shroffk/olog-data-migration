package org.phoebus.old.olog;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import com.sun.jersey.multipart.impl.MultiPartWriter;
import org.phoebus.olog.LogRetrieval;
import org.phoebus.olog.entity.Attachment;
import org.phoebus.olog.entity.Attribute;
import org.phoebus.olog.entity.Log;
import org.phoebus.olog.entity.Log.LogBuilder;
import org.phoebus.olog.entity.Logbook;
import org.phoebus.olog.entity.Property;
import org.phoebus.olog.entity.State;
import org.phoebus.olog.entity.Tag;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.io.InputStreamResource;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriBuilder;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class OldOlogLogRetrieval implements LogRetrieval
{
    private WebResource service;

    private String olog_url="https://olog-acc.nsls2.bnl.gov/";

    private URI ologURI;

    public static ObjectMapper logEntryDeserializer = new ObjectMapper().registerModule(new JavaTimeModule());

    public OldOlogLogRetrieval()
    {

        System.out.println("creating old olog retrieval: " + olog_url);
        this.ologURI = URI.create(olog_url);

        DefaultClientConfig clientConfig = new DefaultClientConfig();
        clientConfig.getClasses().add(MultiPartWriter.class);
        Client client = Client.create(clientConfig);
        client.setFollowRedirects(true);
        service = client.resource(UriBuilder.fromUri(ologURI).build());
    }


    @Override
    public List<Tag> retrieveTags()
    {
        try {
            String tagsString = service.path("Olog/resources/tags").accept(MediaType.APPLICATION_JSON).get(String.class);
            tagsString = tagsString.replace("tag", "tags");
            XmlTags allXmlTags = logEntryDeserializer.readValue(tagsString, XmlTags.class);
            //XmlTags allXmlTags = service.path("Olog/resources/tags").accept(MediaType.APPLICATION_XML).get(XmlTags.class);
            return allXmlTags.getTags().stream().map((xmlTag) -> new Tag(xmlTag.getName(),
                    xmlTag.getState().equalsIgnoreCase("Active") ? State.Active : State.Inactive)).collect(Collectors.toList());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return Collections.emptyList();
    }

    @Override
    public List<Logbook> retrieveLogbooks()
    {
        XmlLogbooks allXmlLogbooks = null;
        try {
            String logbooksString = service.path("Olog/resources/logbooks").accept(MediaType.APPLICATION_JSON)
                    .get(String.class);
            logbooksString = logbooksString.replace("logbook", "logbooks");
            allXmlLogbooks = logEntryDeserializer.readValue(logbooksString, XmlLogbooks.class);
            return allXmlLogbooks.getLogbooks().stream().map((xmlLogbook) -> new Logbook(xmlLogbook.getName(), xmlLogbook.getOwner())).collect(Collectors.toList());
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return Collections.emptyList();
    }

    @Override
    public List<Property> retrieveProperties() {
        try {
            String propertiesString = service.path("Olog/resources/properties").accept(MediaType.APPLICATION_JSON)
                .get(String.class);
            propertiesString = propertiesString.replace("property", "properties");
            XmlProperties allXmlProperties =
                    logEntryDeserializer.readValue(propertiesString, XmlProperties.class);
            return allXmlProperties.getProperties().stream().map((xmlProperty) -> {
                Property property = new Property(xmlProperty.getName());
                property.addAttributes(xmlProperty.getAttributes().entrySet().stream().map((xmlAttribute) -> new Attribute(xmlAttribute.getKey())).collect(Collectors.toSet()));
                return property;
            }).collect(Collectors.toList());
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return Collections.emptyList();
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
    public List<Log> retrieveLogs(int size, int page)
    {

        // Since olog return the log in reverse order in order to get them in the order
        // of ascending
        // id's we need to reverse map them.
        int count = retireveLogCount();
        int mappedPage = ((int)Math.ceil((float)count/(float)size)) + 1 - page;

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.add("limit", String.valueOf(size));
        map.add("page", String.valueOf(mappedPage));

        try {
            String logString = service.path("Olog/resources/logs").queryParams(map).accept(MediaType.APPLICATION_JSON)
                    .accept(MediaType.APPLICATION_JSON).get(String.class);

            logString = logString.replace("log", "logs");
            System.out.println(logString);

//            XmlLog[] tst = logEntryDeserializer.readValue(logString, XmlLog[].class);

            List<XmlLog> xmlLogs =
                    logEntryDeserializer.readValue(logString, new TypeReference<List<XmlLog>>() { });
            System.out.println(xmlLogs.size());

            xmlLogs.stream().forEach(l -> {
                System.out.println(l);
            });

            List<Log> logs = new ArrayList<Log>();
            logs = xmlLogs.stream().map((xmlLog) -> {
                        LogBuilder log = Log.LogBuilder.createLog(xmlLog.getDescription());
                        log.id(xmlLog.getId());
                        if (xmlLog.getLevel() != null && !xmlLog.getLevel().isBlank())
                            log.level(xmlLog.getLevel());
                        if (xmlLog.getCreatedDate() != null)
                            log.createDate(xmlLog.getCreatedDate().toInstant());
                        if (xmlLog.getOwner() != null)
                            log.owner(xmlLog.getOwner());

                        // map attachments
                        if (xmlLog.getXmlAttachments() != null && !xmlLog.getXmlAttachments().isEmpty())
                        {
                            log.setAttachments(xmlLog.getXmlAttachments().stream().map((xmlAttachment) -> {

                                ClientResponse response = service.path("Olog/resources/attachments").path(xmlLog.getId().toString())
                                        .path(xmlAttachment.getFileName()).get(ClientResponse.class);
                                return new Attachment(new InputStreamResource(response.getEntity(InputStream.class)),
                                        xmlAttachment.getFileName(), xmlAttachment.getContentType());
                            }).collect(Collectors.toSet()));
                        }

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
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        return Collections.emptyList();
    }

}
