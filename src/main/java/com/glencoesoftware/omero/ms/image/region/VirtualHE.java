package com.glencoesoftware.omero.ms.image.region;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.Callable;

import javax.imageio.ImageIO;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;


@Command(
    name = "virtualhe", mixinStandardHelpOptions = true,
    description = "Runs Virtual H&E"
)
public class VirtualHE implements Callable<Void> {

    @Parameters(
            index = "0",
            arity = "1",
            description =
                "path to file with image data"
        )
    private String file;


    private static final Logger log =
            LoggerFactory.getLogger(VirtualHE.class);

    private enum ConversionAlgorithm {
        NO_MAPPING,
        VIRTUAL_TRANSILLUMINATION,
        TAO,
        EQN1
    }

    public int virtualTransillumination(double r, double g, double K) {
        double tmpR= (Math.exp(-K*g*0.050)-0.0821)*1.0894;
        double tmpG= (Math.exp(-K*g*1.000)-0.0821)*1.0894;
        double tmpB= (Math.exp(-K*g*0.544)-0.0821)*1.0894;

        tmpR = tmpR*(Math.exp(-K*r*0.860)-0.0821)*1.0894;
        tmpG = tmpG*(Math.exp(-K*r*1.000)-0.0821)*1.0894;
        tmpB = tmpB*(Math.exp(-K*r*0.300)-0.0821)*1.0894;

        return getRgb(tmpR, tmpG, tmpB);
    }

    public int taoTest(double r, double g) {
        double scale = 5.076/4*.7;
        double tmpR = 0.9 - r*1.0*scale - 0.5* g*scale;
        double tmpG = 1.1 - Math.sqrt(r*1.33*scale + 1.25 * g*scale) ;
        double tmpB = 0.9 - r*0.5*scale  - g*scale;

        return getRgb(tmpR, tmpG, tmpB);
    }

    public int eqn1(double r, double g) {
        double tmpR = 1.0 - r*(1.0-0.3) - g*(1.0-1.00);
        double tmpG = 1.0 - r*(1.0-0.2) - g*(1.0-0.55);
        double tmpB = 1.0 - r*(1.0-1.0) - g*(1.0-0.88);

        return getRgb(tmpR, tmpG, tmpB);
    }

    public int getRgb(short r, short g, short b) {
        int rgb = r/64;
        rgb = (rgb << 8) + g/64;
        rgb = (rgb << 8) + b/64;
        return rgb;
    }


    public int getRgb(double r, double g, double b) {
        int rgb = (int) Math.round(r*255);
        rgb = (rgb << 8) + (int) Math.round(g*255);
        rgb = (rgb << 8) + (int) Math.round(b*255);
        return rgb;
    }

    public void loadImage(int width,
            int height,
            double[][] redPixelValues,
            double [][] greenPixelValues,
            BufferedImage image,
            ConversionAlgorithm alg) {
        for(int i = 0; i < width; i++) {
            for(int j = 0; j < height; j++) {
                int rgb = 0;
                switch(alg) {
                case NO_MAPPING:
                    rgb = getRgb(redPixelValues[i][j], greenPixelValues[i][j], 0);
                    break;
                case VIRTUAL_TRANSILLUMINATION:
                    rgb = virtualTransillumination(redPixelValues[i][j], greenPixelValues[i][j], 2.5);
                    break;
                case TAO:
                    rgb = taoTest(redPixelValues[i][j], greenPixelValues[i][j]);
                    break;
                case EQN1:
                    rgb = eqn1(redPixelValues[i][j], greenPixelValues[i][j]);
                }

                image.setRGB(i, j, rgb);
            }
        }
    }

    public void loadImage(int width,
            int height,
            short[][] redPixelValues,
            short [][] greenPixelValues,
            BufferedImage image) {
        for(int i = 0; i < width; i++) {
            for(int j = 0; j < height; j++) {
                int rgb = getRgb(redPixelValues[i][j], greenPixelValues[i][j], (short) 0);
                image.setRGB(i, j, rgb);
            }
        }
    }

    @Override
    public Void call() throws Exception {
        byte[] bytes = IOUtils.toByteArray(new FileInputStream(file));
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        short[][] redPixelValues = new short[1024][1024];
        short[][] greenPixelValues = new short[1024][1024];
        double[][] redDoubleValues = new double[1024][1024];
        double[][] greenDoubleValues = new double[1024][1024];
        for(int i = 0; i < 1024; i++) {
            for(int j = 0; j < 1024; j++) {
                redPixelValues[i][j] = bb.getShort();
                redDoubleValues[i][j] = ((double) redPixelValues[i][j])/16384.0;//2^14 - the max value
            }
        }
        for(int i = 0; i < 1024; i++) {
            for(int j = 0; j < 1024; j++) {
                greenPixelValues[i][j] = bb.getShort();
                greenDoubleValues[i][j] = ((double) greenPixelValues[i][j])/16384.0;//2^14 - the max value
            }
        }
        /*
        BufferedImage image = new BufferedImage(1024, 1024, BufferedImage.TYPE_INT_RGB);
        loadImage(1024, 1024, redPixelValues, greenPixelValues, image);
        File outputFile = new File("/home/kevin/code/omero-ms-image-region/output.png");
        ImageIO.write(image, "png", outputFile);
        */
        BufferedImage image = new BufferedImage(1024, 1024, BufferedImage.TYPE_INT_RGB);
        loadImage(1024, 1024, redDoubleValues, greenDoubleValues, image, ConversionAlgorithm.VIRTUAL_TRANSILLUMINATION);
        File outputFile = new File("./virtual_transillumination.png");
        ImageIO.write(image, "png", outputFile);
        image = new BufferedImage(1024, 1024, BufferedImage.TYPE_INT_RGB);
        loadImage(1024, 1024, redDoubleValues, greenDoubleValues, image, ConversionAlgorithm.TAO);
        outputFile = new File("./tao.png");
        ImageIO.write(image, "png", outputFile);
        image = new BufferedImage(1024, 1024, BufferedImage.TYPE_INT_RGB);
        loadImage(1024, 1024, redDoubleValues, greenDoubleValues, image, ConversionAlgorithm.EQN1);
        outputFile = new File("./eqn1.png");
        ImageIO.write(image, "png", outputFile);
        log.info("Done!");
        return null;
    }

    public static void main(String[] args) {
        new CommandLine(new VirtualHE()).execute(args);
    }
}
