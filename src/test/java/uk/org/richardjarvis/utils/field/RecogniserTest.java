package uk.org.richardjarvis.utils.field;

import com.google.i18n.phonenumbers.Phonenumber;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Test;
import uk.org.richardjarvis.metadata.text.FieldMeaning;

import java.util.Set;

import static org.junit.Assert.*;

/**
 * Created by rjarvis on 23/03/16.
 */
public class RecogniserTest {

    @Test
    public void getPossibleMeaningDate1() throws Exception {
        FieldMeaning textMeaning = new FieldMeaning(FieldMeaning.MeaningType.TEXT, null, DataTypes.StringType);
        Set<FieldMeaning> meanings = Recogniser.getPossibleMeanings("27/05/1979 08:30:31");
        assertTrue(meanings.contains(new FieldMeaning(FieldMeaning.MeaningType.DATE, "dd/MM/yyyy HH:mm:ss", DataTypes.StringType)));
        assertTrue(meanings.contains(textMeaning));
        assertEquals(2, meanings.size());
    }

    @Test
    public void getPossibleMeaningDate2() throws Exception {
        FieldMeaning textMeaning = new FieldMeaning(FieldMeaning.MeaningType.TEXT, null, DataTypes.StringType);
        Set<FieldMeaning> meanings = Recogniser.getPossibleMeanings("06/05/1979 08:30:31");
        assertTrue(meanings.contains(new FieldMeaning(FieldMeaning.MeaningType.DATE, "dd/MM/yyyy HH:mm:ss", DataTypes.StringType)));
        assertTrue(meanings.contains(new FieldMeaning(FieldMeaning.MeaningType.DATE, "MM/dd/yyyy HH:mm:ss", DataTypes.StringType)));
        assertTrue(meanings.contains(textMeaning));
        assertEquals(3, meanings.size());
    }

    @Test
    public void getPossibleMeaningDate3() throws Exception {
        FieldMeaning textMeaning = new FieldMeaning(FieldMeaning.MeaningType.TEXT, null, DataTypes.StringType);
        Set<FieldMeaning> meanings = Recogniser.getPossibleMeanings("2016-04-28 17:15:31");
        assertTrue(meanings.contains(new FieldMeaning(FieldMeaning.MeaningType.DATE, "yyyy-MM-dd HH:mm:ss", DataTypes.StringType)));
        assertTrue(meanings.contains(textMeaning));
        assertEquals(2, meanings.size());
    }

    @Test
    public void getPossibleMeaningIP() throws Exception {
        FieldMeaning textMeaning = new FieldMeaning(FieldMeaning.MeaningType.TEXT, null, DataTypes.StringType);
        Set<FieldMeaning> meanings = Recogniser.getPossibleMeanings("192.168.1.1");
        assertTrue(meanings.contains(new FieldMeaning(FieldMeaning.MeaningType.IPv4, null, DataTypes.StringType)));
        assertTrue(meanings.contains(new FieldMeaning(FieldMeaning.MeaningType.PHONE_NUMBER, "AT|RS|BE|SE|SI|CO|DZ|FI|HR|HU|IE|IQ|KH|MM|NG|PE", DataTypes.StringType)));
        assertTrue(meanings.contains(textMeaning));
        assertEquals(3, meanings.size());
    }

    @Test
    public void getPossibleMeaningURL1() throws Exception {
        FieldMeaning textMeaning = new FieldMeaning(FieldMeaning.MeaningType.TEXT, null, DataTypes.StringType);
        Set<FieldMeaning> meanings = Recogniser.getPossibleMeanings("http://www.google.com");
        assertTrue(meanings.contains(new FieldMeaning(FieldMeaning.MeaningType.URL, null, DataTypes.StringType)));
        assertTrue(meanings.contains(textMeaning));
        assertEquals(2, meanings.size());
    }

    @Test
    public void getPossibleMeaningURL2() throws Exception {
        FieldMeaning textMeaning = new FieldMeaning(FieldMeaning.MeaningType.TEXT, null, DataTypes.StringType);
        Set<FieldMeaning> meanings = Recogniser.getPossibleMeanings("ftp://dropbox.com");
        assertTrue(meanings.contains(new FieldMeaning(FieldMeaning.MeaningType.URL, null, DataTypes.StringType)));
        assertTrue(meanings.contains(textMeaning));
        assertEquals(2, meanings.size());
    }

    @Test
    public void getPossibleMeaningURL3() throws Exception {
        FieldMeaning textMeaning = new FieldMeaning(FieldMeaning.MeaningType.TEXT, null, DataTypes.StringType);
        Set<FieldMeaning> meanings = Recogniser.getPossibleMeanings("https://www.google.com");
        assertTrue(meanings.contains(new FieldMeaning(FieldMeaning.MeaningType.URL, null, DataTypes.StringType)));
        assertTrue(meanings.contains(textMeaning));
        assertEquals(2, meanings.size());
    }

    @Test
    public void getPossibleMeaningEmail() throws Exception {
        FieldMeaning textMeaning = new FieldMeaning(FieldMeaning.MeaningType.TEXT, null, DataTypes.StringType);
        Set<FieldMeaning> meanings = Recogniser.getPossibleMeanings("richard.jarvis@testing.com");
        assertTrue(meanings.contains(new FieldMeaning(FieldMeaning.MeaningType.EMAIL_ADDRESS, null, DataTypes.StringType)));
        assertTrue(meanings.contains(textMeaning));
        assertEquals(2, meanings.size());
    }

    @Test
    public void getPossibleMeaningMacAddress() throws Exception {
        FieldMeaning textMeaning = new FieldMeaning(FieldMeaning.MeaningType.TEXT, null, DataTypes.StringType);
        Set<FieldMeaning> meanings = Recogniser.getPossibleMeanings("12:34:45:AA:BF:23");
        assertTrue(meanings.contains(new FieldMeaning(FieldMeaning.MeaningType.MAC_ADDRESS, null, DataTypes.StringType)));
        assertTrue(meanings.contains(textMeaning));
        assertEquals(2, meanings.size());
    }

    @Test
    public void getPossibleMeaningText() throws Exception {
        FieldMeaning textMeaning = new FieldMeaning(FieldMeaning.MeaningType.TEXT, null, DataTypes.StringType);
        Set<FieldMeaning> meanings = Recogniser.getPossibleMeanings("A String");
        assertTrue(meanings.contains(textMeaning));
        assertEquals(1, meanings.size());
    }

    @Test
    public void getPossibleMeaningNumber() throws Exception {
        FieldMeaning textMeaning = new FieldMeaning(FieldMeaning.MeaningType.TEXT, null, DataTypes.StringType);
        Set<FieldMeaning> meanings = Recogniser.getPossibleMeanings("123456");
        assertTrue(meanings.contains(new FieldMeaning(FieldMeaning.MeaningType.NUMERIC, null, DataTypes.IntegerType)));
        assertTrue(meanings.contains(textMeaning));
        assertEquals(3, meanings.size());
    }

    @Test
    public void getPossibleMeaningPhoneNumber() throws Exception {
        FieldMeaning textMeaning = new FieldMeaning(FieldMeaning.MeaningType.TEXT, null, DataTypes.StringType);
        Set<FieldMeaning> meanings = Recogniser.getPossibleMeanings("01234567890");
        assertTrue(meanings.contains(new FieldMeaning(FieldMeaning.MeaningType.NUMERIC, null, DataTypes.IntegerType)));
        assertTrue(meanings.contains(new FieldMeaning(FieldMeaning.MeaningType.PHONE_NUMBER, "AT|RS|BR|VN|GB|IN|IT|KR", DataTypes.StringType)));
        assertTrue(meanings.contains(textMeaning));
        assertEquals(3, meanings.size());
    }

    @Test
    public void getPossibleMeaningPhoneNumber2() throws Exception {
        FieldMeaning textMeaning = new FieldMeaning(FieldMeaning.MeaningType.TEXT, null, DataTypes.StringType);
        Set<FieldMeaning> meanings = Recogniser.getPossibleMeanings("+441234567890");
        assertTrue(meanings.contains(new FieldMeaning(FieldMeaning.MeaningType.PHONE_NUMBER, "GB", DataTypes.StringType)));
        assertTrue(meanings.contains(textMeaning));
        assertEquals(2, meanings.size());
    }
}