package uk.org.richardjarvis.utils.report;

import org.junit.Test;
import uk.org.richardjarvis.utils.file.FileUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by rjarvis on 22/04/16.
 */
public class ReportUtilTest {
    @Test
    public void buildReportFromMap() throws Exception {

        Random random =new Random();

        Map<String,Integer> map = new HashMap<>();

        for (int i=0; i< 10; i++) {
            map.put("Key_" + random.nextInt(), random.nextInt());
        }

        StringBuilder sb = new StringBuilder();

        ReportUtil.addHTMLHeader(sb,"Text File Report");
        ReportUtil.addBodyStart(sb);
        ReportUtil.addHTMLTable(sb,"Data Table", map);
        ReportUtil.addBodyEnd(sb);
        ReportUtil.addHTMLFooter(sb);

        FileUtils.writeBufferToFile(sb,"/tmp/report.html");

    }
}