package uk.org.richardjarvis.utils.image;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by rjarvis on 21/04/16.
 */
public class ColourNamerTest {

    @Test
    public void namerTest() throws Exception {

        assertEquals("Salomie", ColourNamer.getColourName(0.84313, 1, 0.45).getName());
        assertEquals("Frangipani", ColourNamer.getColourName(0.5657894737, 1, 0.8509803922).getName());
        assertEquals("Yellow", ColourNamer.getColourName(1, 1, 0.5).getName());
        assertEquals("Blue", ColourNamer.getColourName(4, 1, 0.5).getName());

    }
}