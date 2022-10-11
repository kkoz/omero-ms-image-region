/*
 * Copyright (C) 2022 Glencoe Software, Inc. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

package com.glencoesoftware.omero.ms.image.region;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ome.util.PixelData;
import omeis.providers.re.ColorsFactory;
import omeis.providers.re.Optimizations;
import omeis.providers.re.RGBBuffer;
import omeis.providers.re.RenderingTask;
import omeis.providers.re.codomain.CodomainChain;
import omeis.providers.re.lut.LutReader;
import omeis.providers.re.quantum.BinaryMaskQuantizer;
import omeis.providers.re.quantum.QuantizationException;
import omeis.providers.re.quantum.QuantumStrategy;
import omeis.providers.re.RGBIntBuffer;

public class RGBInterleavedRenderingTask implements RenderingTask {

    /** The logger for this particular class */
    private static Logger log = LoggerFactory.getLogger(RGBInterleavedRenderingTask.class);

    /** Buffer to hold the output image's data. */
    private RGBBuffer dataBuffer;

    private PixelData pixelData;

    /** How to quantize a pixel intensity value. */
    private List<QuantumStrategy> strategies;

    /**
     * The spatial transformations to apply to the quantized data.
     * One per channel.
     */
    private List<CodomainChain> chains;

    /**
     * The color components used when mapping a quantized value onto the color
     * space.
     */
    private List<int[]> colors;

    /** The <i>X1</i>-axis start */
    private int x1Start;

    /** The <i>X1</i>-axis end */
    private int x1End;

    /** The <i>X2</i>-axis start */
    private int x2Start;

    /** The <i>X2</i>-axis end */
    private int x2End;

    /** The optimizations that the renderer has turned on for us. */
    private Optimizations optimizations;

    /** The collection of readers.*/
    private List<LutReader> lutReaders;

    RGBInterleavedRenderingTask(RGBBuffer dataBuffer, PixelData pixelData,
            List<QuantumStrategy> strategies, List<CodomainChain> chains,
            List<int[]> colors, Optimizations optimizations,
            int x1Start, int x1End, int x2Start, int x2End,
            List<LutReader> lutReaders) {
        this.dataBuffer = dataBuffer;
        this.pixelData = pixelData;
        this.strategies = strategies;
        this.chains = chains;
        this.colors = colors;
        this.optimizations = optimizations;
        this.x1Start = x1Start;
        this.x1End = x1End;
        this.x2Start = x2Start;
        this.x2End = x2End;
        this.lutReaders = lutReaders;
    }

    @Override
    public Object call() throws QuantizationException {
        // TODO Currently always rendering as packed int
        renderPackedInt();
        return null;
    }

    private void renderPackedInt() throws QuantizationException {

        int discreteValue;
        double redRatio, greenRatio, blueRatio;
        int rValue, gValue, bValue;
        int newRValue, newGValue, newBValue;
        int colorOffset = 24;  // Only used when we're doing primary color.

        int width = x1End - x1Start;
        int[] intBuf = ((RGBIntBuffer) dataBuffer).getDataBuffer();
        boolean isPrimaryColor = optimizations.isPrimaryColorEnabled();
        boolean isAlphaless = optimizations.isAlphalessRendering();
        LutReader lutReader;
        CodomainChain cc;
        int bytesPerPixel = pixelData.bytesPerPixel();

        for (int x2 = x2Start; x2 < x2End; ++x2) {
            for (int x1 = x1Start; x1 < x1End; ++x1) {
                for (int j = 0; j < 3; j++) {

                    int pix = width * x2 + x1;
                    int[] color = colors.get(j);
                    lutReader = lutReaders.get(j);
                    cc = chains.get(j);
                    boolean hasMap = cc.hasMapContext();
                    QuantumStrategy qs = strategies.get(j);
                    boolean isMask = qs instanceof BinaryMaskQuantizer? true : false;
                    redRatio = color[ColorsFactory.RED_INDEX] > 0 ?
                            color[ColorsFactory.RED_INDEX] / 255.0 : 0.0;
                    greenRatio = color[ColorsFactory.GREEN_INDEX] > 0 ?
                             color[ColorsFactory.GREEN_INDEX] / 255.0 : 0.0;
                    blueRatio = color[ColorsFactory.BLUE_INDEX] > 0 ?
                             color[ColorsFactory.BLUE_INDEX] / 255.0 : 0.0;
                    // Get our color offset if we've got the primary color optimization
                    // enabled.
                    if (isPrimaryColor && lutReader == null)
                        colorOffset = getColorOffset(color);
                    float alpha = new Integer(
                            color[ColorsFactory.ALPHA_INDEX]).floatValue() / 255;

                    // TODO: We currently assume XY-Plane
                    // TODO: Assuming 3 channels
                     discreteValue =
                     qs.quantize(
                             pixelData.getPixelValueDirect(((pix*3) + j) * bytesPerPixel));

                     if (hasMap) {
                         discreteValue = cc.transform(discreteValue);
                     }
                     if (lutReader != null) {
                         int r1 = ((intBuf[pix] & 0x00FF0000) >> 16);
                         int r2 = lutReader.getRed(discreteValue) & 0xFF;
                         int g1 = ((intBuf[pix] & 0x0000FF00) >> 8);
                         int g2 = lutReader.getGreen(discreteValue) & 0xFF;
                         int b1 = (intBuf[pix] & 0x000000FF);
                         int b2 = lutReader.getBlue(discreteValue) & 0xFF;
                         int r = r1+r2;
                         if (r > 255) {
                             r = 255;
                         }
                         int g = g1+g2;
                         if (g > 255) {
                             g = 255;
                         }
                         int b = b1+b2;
                         if (b > 255) {
                             b = 255;
                         }
                         intBuf[pix] = 0xFF000000 | r << 16 | g << 8 | b;
                         continue;
                     }
                     // Primary colour optimization is in effect, we don't need
                     // to do any of the sillyness below just shift the value
                     // into the correct colour component slot and move on to
                     // the next pixel value.
                     if (colorOffset != 24)
                     {
                         intBuf[pix] |= 0xFF000000;  // Alpha.
                         intBuf[pix] |= discreteValue << colorOffset;
                         continue;
                     }
                     newRValue = (int) (redRatio * discreteValue);
                     newGValue = (int) (greenRatio * discreteValue);
                     newBValue = (int) (blueRatio * discreteValue);

                     // Pre-multiply the alpha for each colour component if the
                     // image has a non-1.0 alpha component.
                     if (!isAlphaless)
                     {
                         newRValue *= alpha;
                         newGValue *= alpha;
                         newBValue *= alpha;
                     }

                     if (isMask && discreteValue == 255) {
                         // Since the mask is a hard value, we do not want to
                         // compromise on colour fidelity. Packed each colour
                         // component along with a 1.0 alpha into the buffer so
                         // that buffered images that use this buffer can be
                         // type 1 (3 bands, pre-multiplied alpha) or type 2
                         // (4 bands, alpha component included).
                         intBuf[pix] = 0xFF000000 | newRValue << 16
                                    | newGValue << 8 | newBValue;
                         continue;
                     }
                     // Add the existing colour component values to the new
                     // colour component values.
                     rValue = ((intBuf[pix] & 0x00FF0000) >> 16) + newRValue;
                     gValue = ((intBuf[pix] & 0x0000FF00) >> 8) + newGValue;
                     bValue = (intBuf[pix] & 0x000000FF) + newBValue;

                     // Ensure that each colour component value is between 0 and
                     // 255 (byte). We must make *certain* that values do not
                     // wrap over 255 otherwise there will be corruption
                     // introduced into the rendered image. The value may be over
                     // 255 if we have mapped two high intensity channels to
                     // the same color.
                     if (rValue > 255) {
                         rValue = 255;
                     }
                     if (gValue > 255) {
                         gValue = 255;
                     }
                     if (bValue > 255) {
                         bValue = 255;
                     }

                     // Packed each colour component along with a 1.0 alpha into
                     // the buffer so that buffered images that use this buffer
                     // can be type 1 (3 bands, pre-multiplied alpha) or type 2
                     // (4 bands, alpha component included).
                     intBuf[pix] = 0xFF000000 | rValue << 16 | gValue << 8 | bValue;
                }
            }
        }
    }

    /**
     * Returns a color offset based on which color component is 0xFF.
     * @param color the color to check.
     * @return an integer color offset in bits.
     */
    private int getColorOffset(int[] color)
    {
        if (color[ColorsFactory.RED_INDEX] == 255)
            return 16;
        if (color[ColorsFactory.GREEN_INDEX] == 255)
            return 8;
        if (color[ColorsFactory.BLUE_INDEX] == 255)
            return 0;
        throw new IllegalArgumentException(
                "Unable to find color component offset in color.");
    }

}
