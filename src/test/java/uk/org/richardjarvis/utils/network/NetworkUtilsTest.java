package uk.org.richardjarvis.utils.network;

import org.junit.Test;
import scala.Tuple2;
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

    @Test
    public void testGetSubnetBounds() throws Exception {

        String testNet = "10.0.0.0/8";

        Tuple2<Long, Long> bounds = NetworkUtils.getSubnetBounds(testNet);

        assertEquals((Long)167772160l,bounds._1);
        assertEquals((Long)184549375l,bounds._2);

        testNet = "216.54.213.13/32";

        bounds = NetworkUtils.getSubnetBounds(testNet);

        assertEquals((Long)3627472141l,bounds._1);
        assertEquals((Long)3627472141l,bounds._2);

    }

    @Test
    public void loadGeoFileIndex() throws Exception {
        NetworkUtils.loadGeoFileIndex();

    }

    @Test
    public void getAddress() throws Exception {
        long ip = NetworkUtils.getAddress("212.183.100.100");
        assertEquals(3568788580l,ip);
    }
}