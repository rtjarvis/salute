package uk.org.richardjarvis.processor.audio;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by rjarvis on 11/03/16.
 */
public class DiscreteFourierTransform {

    static void dft(double[] inR, double[] outR, double[] outI) {
        for (int k = 0; k < inR.length; k++) {
            for (int t = 0; t < inR.length; t++) {
                outR[k] += inR[t]*Math.cos(2*Math.PI * t * k / inR.length);
                outI[k] -= inR[t]*Math.sin(2*Math.PI * t * k / inR.length);
            }
        }
    }

    static List<Double> process(double results[], double sampleRate, int numSamples, int sigma) {
        double average = 0;
        for (int i = 0; i < results.length; i++) {
            average += results[i];
        }
        average = average/results.length;

        double sums = 0;
        for (int i = 0; i < results.length; i++) {
            sums += (results[i]-average)*(results[i]-average);
        }

        double stdev = Math.sqrt(sums/(results.length-1));

        ArrayList<Double> found = new ArrayList<>();
        double max = Integer.MIN_VALUE;
        int maxF = -1;
        for (int f = 0; f < results.length/2; f++) {
            if (results[f] > average+sigma*stdev) {
                if (results[f] > max) {
                    max = results[f];
                    maxF = f;
                }
            } else {
                if (maxF != -1) {
                    found.add(maxF*sampleRate/numSamples);
                    max = Integer.MIN_VALUE;
                    maxF = -1;
                }
            }
        }

        return (found);
    }
}

