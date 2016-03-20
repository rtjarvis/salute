package uk.org.richardjarvis.utils.network;

import org.junit.Test;
import uk.org.richardjarvis.derive.tabular.GeoIPDeriver;

import static org.junit.Assert.*;

/**
 * Created by rjarvis on 19/03/16.
 */
public class NetworkUtilsTest {

    @Test
    public void testNetMatch() throws Exception {
        String testIP = "10.0.0.4";
        String testNet = "10.0.0.0/8";

        boolean match = NetworkUtils.netMatch(testIP, testNet);

        assertTrue(match);

    }
}