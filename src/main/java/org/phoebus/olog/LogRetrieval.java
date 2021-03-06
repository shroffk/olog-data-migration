package org.phoebus.olog;

import java.util.List;

import org.phoebus.olog.entity.Property;
import org.phoebus.olog.entity.Tag;
import org.phoebus.olog.entity.Log;
import org.phoebus.olog.entity.Logbook;

/**
 * A service SPI for retrieving data for the old log sourc
 * 
 * @author kunal
 *
 */
public interface LogRetrieval
{

    public List<Tag> retrieveTags();

    public List<Property> retrieveProperties();

    public List<Logbook> retrieveLogbooks();

    public List<Log> retrieveLogs(int size, int page);

    int retireveLogCount();

}
