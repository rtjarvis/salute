package uk.org.richardjarvis.utils.report;

import jdk.nashorn.api.scripting.URLReader;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.tika.metadata.Metadata;
import uk.org.richardjarvis.utils.DataFrameUtils;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by rjarvis on 22/04/16.
 */
public class ReportUtil {

    private static final String NULL_STRING = "<i>[NULL]</i>";

    public static void addHTMLHeader(StringBuilder stringBuilder, String title) {

        List<URL> defaultStylesheets = new ArrayList<>();
        defaultStylesheets.add(ReportUtil.class.getResource("/dataTable.css"));
        defaultStylesheets.add(ReportUtil.class.getResource("/header.css"));

        addHTMLHeader(stringBuilder, title, defaultStylesheets);
    }

    public static void addHTMLHeader(StringBuilder stringBuilder, String title, List<URL> stylesheets) {

        stringBuilder.append("<html lang=\"en\">");
        stringBuilder.append("<head>");
        stringBuilder.append("<meta charset=\"utf-8\" />");
        stringBuilder.append("<title>");
        stringBuilder.append(title);
        stringBuilder.append("</title>");
        stringBuilder.append("<meta name=\"viewport\" content=\"initial-scale=1.0; maximum-scale=1.0; width=device-width;\">");
        if (stylesheets != null) {
            stringBuilder.append("<style>");
            for (URL stylesheet : stylesheets) {
                BufferedReader bufferedReader = new BufferedReader(new URLReader(stylesheet));
                bufferedReader.lines().forEach(s -> stringBuilder.append(s));
            }
            stringBuilder.append("</style>");
        }
        stringBuilder.append("</head>");
    }

    public static void addApplicationHeader(StringBuilder stringBuilder) {

        stringBuilder.append("<header class=\"header-basic-light\">");
        stringBuilder.append("<div class=\"header-limiter\">");
        stringBuilder.append("<a href=\"#\"><h1>Salute</h1></a>");
        stringBuilder.append("<nav>");
        stringBuilder.append("<a href=\"#\">Home</a>");
        stringBuilder.append("</nav>");
        stringBuilder.append("</div>");
        stringBuilder.append("</header>");

    }

    public static void addHTMLTable(StringBuilder stringBuilder, String tableTitle, Map<? extends Object, ? extends Object> data) {

        addTableTitle(stringBuilder, tableTitle);
        addTableHeader(stringBuilder, Arrays.asList("Key", "Value"));

        addStartTableBody(stringBuilder);
        for (Object title : data.keySet()) {
            addTableRow(stringBuilder, Arrays.asList(title, data.get(title)));
        }
        addEndTableBody(stringBuilder);

    }

    public static void addHTMLTable(StringBuilder stringBuilder, String tableTitle, Metadata data) {

        if (data == null)
            return;

        addTableTitle(stringBuilder, tableTitle);
        addTableHeader(stringBuilder, Arrays.asList("Key", "Value"));

        addStartTableBody(stringBuilder);
        for (String title : data.names()) {
            addTableRow(stringBuilder, Arrays.asList(title, data.get(title)));
        }
        addEndTableBody(stringBuilder);

    }

    public static void addHTMLTable(StringBuilder stringBuilder, String tableTitle, DataFrame data, int rowCount) {

        if (data == null)
            return;

        List<Row> rows = DataFrameUtils.getSampleHead(data, rowCount);

        List<String> fieldNames = Arrays.asList(data.schema().fieldNames());
        addTableTitle(stringBuilder, tableTitle);

        addTableHeader(stringBuilder, fieldNames, "table-small");

        addStartTableBody(stringBuilder);
        for (Row row : rows) {
            List<Object> rowData = new ArrayList<>();
            for (int i = 0; i < row.size(); i++) {
                rowData.add(row.get(i));
            }
            addTableRow(stringBuilder, rowData);
        }
        addEndTableBody(stringBuilder);

    }

    public static void addHTMLTable(StringBuilder stringBuilder, String tableTitle, List<Object> data) {

        if (data == null)
            return;

        addTableTitle(stringBuilder, tableTitle);
        addTableHeader(stringBuilder, Arrays.asList("", "Value"));

        addStartTableBody(stringBuilder);
        int i = 0;
        for (Object datum : data) {
            addTableRow(stringBuilder, Arrays.asList(i, datum));
        }
        addEndTableBody(stringBuilder);

    }

    private static void addEndTableBody(StringBuilder stringBuilder) {
        stringBuilder.append("</tbody></table>");
    }

    private static void addStartTableBody(StringBuilder stringBuilder) {
        stringBuilder.append("<tbody class=\"table-hover\">");
    }

    private static void addTableRow(StringBuilder stringBuilder, List<Object> tableData) {

        stringBuilder.append("<tr>\n");
        for (Object tableDatum : tableData) {
            stringBuilder.append("<td class=\"text-left\">");
            stringBuilder.append((tableDatum == null) ? NULL_STRING : tableDatum.toString());
            stringBuilder.append("</td>\n");
        }
        stringBuilder.append("</tr>\n");
    }

    private static void addTableHeader(StringBuilder stringBuilder, List<String> headerTitles) {
        addTableHeader(stringBuilder, headerTitles, "table-fill");
    }

    private static void addTableHeader(StringBuilder stringBuilder, List<String> headerTitles, String cssClass) {

        stringBuilder.append("<table class=\"");
        stringBuilder.append(cssClass);
        stringBuilder.append("\">");
        stringBuilder.append("<thead><tr>");
        for (String headerTitle : headerTitles) {
            stringBuilder.append("<th class=\"text-left\">");
            stringBuilder.append(headerTitle);
            stringBuilder.append("</th>\n");
        }
        stringBuilder.append("</tr></thead>");
    }


    private static void addTableTitle(StringBuilder stringBuilder, String tableTitle) {
        stringBuilder.append("<div class=\"centre-block\"><h3>");
        stringBuilder.append(tableTitle);
        stringBuilder.append("</h3></div>");
    }

    public static void addLinkToFile(StringBuilder stringBuilder, String path) {

        stringBuilder.append("<div class=\"centre-block\">");
        stringBuilder.append("<h3>Source File</h3><p><a href=\"");
        stringBuilder.append(path);
        stringBuilder.append("\">");
        stringBuilder.append(path);
        stringBuilder.append("</a>");
        stringBuilder.append("</p></div>");

    }

    public static void addImage(StringBuilder stringBuilder, String imagePath) {

        stringBuilder.append("<div class=\"centre-block\"><p>");
        stringBuilder.append("<img src=\"");
        stringBuilder.append(imagePath);
        stringBuilder.append("\">");
        stringBuilder.append("</p></div>");

    }

    public static void addBodyStart(StringBuilder stringBuilder) {

        stringBuilder.append("<body>");
        addApplicationHeader(stringBuilder);

    }

    public static void addBodyEnd(StringBuilder stringBuilder) {

        stringBuilder.append("</body>");

    }

    public static void addHTMLFooter(StringBuilder sb) {
        sb.append("</html>");
    }
}
