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

import java.awt.Color;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import loci.formats.FormatException;
import loci.formats.FormatTools;
import ome.conditions.ResourceError;
import ome.io.bioformats.BfPixelBuffer;
import ome.model.core.Pixels;
import ome.model.display.ChannelBinding;
import ome.model.display.QuantumDef;
import ome.model.enums.PixelsType;
import ome.util.PixelData;
import omeis.providers.re.ColorsFactory;
import omeis.providers.re.QuantumManager;
import omeis.providers.re.RGBAIntBuffer;
import omeis.providers.re.RGBBuffer;
import omeis.providers.re.RGBIntBuffer;
import omeis.providers.re.Renderer;
import omeis.providers.re.RenderingStats;
import omeis.providers.re.RenderingStrategy;
import omeis.providers.re.RenderingTask;
import omeis.providers.re.codomain.CodomainChain;
import omeis.providers.re.data.PlaneDef;
import omeis.providers.re.data.RegionDef;
import omeis.providers.re.lut.LutReader;
import omeis.providers.re.quantum.BinaryMaskQuantizer;
import omeis.providers.re.quantum.QuantizationException;
import omeis.providers.re.quantum.QuantumStrategy;

public class RGBInterleavedStrategy extends RenderingStrategy {

    /** The logger for this particular class */
    private static Logger log = LoggerFactory.getLogger(RGBInterleavedStrategy.class);

    @Override
    protected RGBBuffer render(Renderer ctx, PlaneDef planeDef)
            throws IOException, QuantizationException {
        // Set the context and retrieve objects we're gonna use.
        renderer = ctx;
        //RenderingStats performanceStats = renderer.getStats();
        Pixels metadata = renderer.getMetadata();

        // Initialize sizeX1 and sizeX2 according to the plane definition and
        // create the RGB buffer.
        initAxesSize(planeDef, metadata);
        RGBBuffer buf = getRgbBuffer();
        render(buf, planeDef);
        return buf;
    }

    @Override
    protected RGBIntBuffer renderAsPackedInt(Renderer ctx, PlaneDef planeDef)
            throws IOException, QuantizationException {
        // Set the context and retrieve objects we're gonna use.
        renderer = ctx;
        Pixels metadata = renderer.getMetadata();

        // Initialize sizeX1 and sizeX2 according to the plane definition and
        // create the RGB buffer.
        initAxesSize(planeDef, metadata);
        RGBIntBuffer buf = getIntBuffer();
        render(buf, planeDef);
        return buf;
    }

    /**
     * Retrieves the quantum strategy for each active channels
     *
     * @return the active channel color data.
     */
    private List<QuantumStrategy> getStrategies() {
        ChannelBinding[] channelBindings = renderer.getChannelBindings();
        QuantumManager qManager = renderer.getQuantumManager();
        List<QuantumStrategy> strats = new ArrayList<QuantumStrategy>();

        for (int w = 0; w < channelBindings.length; w++) {
            if (channelBindings[w].getActive()) {
                strats.add(qManager.getStrategyFor(w));
            }
        }
        Map<byte[], Integer> overlays = renderer.getOverlays();
        if (overlays != null)
        {
            QuantumDef def = new QuantumDef();  // Just to fulfill interface
            Pixels pixels = new Pixels();
            PixelsType bitType = new PixelsType();
            bitType.setValue(PixelsType.VALUE_BIT);
            bitType.setBitSize(1);
            pixels.setPixelsType(bitType);
            for (int i = 0; i < overlays.size(); i++)
            {
                strats.add(new BinaryMaskQuantizer(def, pixels));
            }
        }
        return strats;
    }

    /**
     * Retrieves the color for each active channels.
     *
     * @return the active channel color data.
     */
    private List<int[]> getColors() {
        ChannelBinding[] channelBindings = renderer.getChannelBindings();
        List<int[]> colors = new ArrayList<int[]>();

        for (int w = 0; w < channelBindings.length; w++) {
            ChannelBinding cb = channelBindings[w];
            if (cb.getActive()) {
                int[] theNewColor = new int[] {
                        cb.getRed(), cb.getGreen(),
                        cb.getBlue(), cb.getAlpha() };
                colors.add(theNewColor);
            }
        }
        Map<byte[], Integer> overlays = renderer.getOverlays();
        if (overlays != null)
        {
            for (byte[] overlay : overlays.keySet())
            {
                Integer packedColor = overlays.get(overlay);
                Color color = new Color(packedColor);
                colors.add(new int[] { color.getRed(), color.getBlue(),
                                       color.getGreen(), color.getAlpha() });
            }
        }
        return colors;
    }

    /**
     * Returns the collection of chains.
     *
     * @return See above.
     */
    private List<CodomainChain> getChains()
    {
        List<CodomainChain> chains = renderer.getCodomainChains();
        ChannelBinding[] channelBindings = renderer.getChannelBindings();
        List<CodomainChain> list = new ArrayList<CodomainChain>();
        for (int w = 0; w < channelBindings.length; w++) {
            ChannelBinding cb = channelBindings[w];
            if (cb.getActive()) {
                list.add(chains.get(w));
            }
        }
        return list;
    }

    private void renderSingleThreaded(RGBBuffer buf, PlaneDef planeDef) throws IOException,
    QuantizationException {
        RenderingStats performanceStats = renderer.getStats();

        BfPixelBuffer pixelBuffer = (BfPixelBuffer) renderer.getPixels();

        int discreteValue;
        double redRatio, greenRatio, blueRatio;
        int rValue, gValue, bValue;
        int newRValue, newGValue, newBValue;
        int colorOffset = 24;  // Only used when we're doing primary color.

        int[] intBuf = ((RGBIntBuffer) buf).getDataBuffer();
        boolean isPrimaryColor = renderer.getOptimizations().isPrimaryColorEnabled();
        boolean isAlphaless = renderer.getOptimizations().isAlphalessRendering();
        List<int[]> colors = getColors();
        List<LutReader> lutReaders = renderer.getLutProvider().getLutReaders(
                renderer.getChannelBindings());
        List<QuantumStrategy> strategies = getStrategies();
        List<CodomainChain> chains = getChains();
        LutReader lutReader;
        CodomainChain cc;
        RegionDef region = planeDef.getRegion();

        byte[] imageData = new byte[region.getWidth() * region.getHeight()
                                 * FormatTools.getBytesPerPixel(PixelsType.VALUE_BIT) * 3];
        pixelBuffer.getTileDirect(0, 0, 0, region.getX(),
                region.getY(),
                region.getWidth(),
                region.getHeight(),
                imageData);

        ByteBuffer bbuf = ByteBuffer.wrap(imageData);
        PixelData pixelData = new PixelData(renderer.getPixelsType().getValue(), bbuf);
        int bytesPerPixel = pixelData.bytesPerPixel();

        for (int i = 0; i < imageData.length/3; i++) {
            for (int j = 0; j < 3; j++) {
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

                 // TODO: Assume XY Planar
                 // TODO: Assume 3 channels
                 discreteValue =
                 qs.quantize(
                         pixelData.getPixelValueDirect(((i*3) + j) * bytesPerPixel));
                 if (hasMap) {
                     discreteValue = cc.transform(discreteValue);
                 }
                 if (lutReader != null) {
                     int r1 = ((intBuf[i] & 0x00FF0000) >> 16);
                     int r2 = lutReader.getRed(discreteValue) & 0xFF;
                     int g1 = ((intBuf[i] & 0x0000FF00) >> 8);
                     int g2 = lutReader.getGreen(discreteValue) & 0xFF;
                     int b1 = (intBuf[i] & 0x000000FF);
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
                     intBuf[i] = 0xFF000000 | r << 16 | g << 8 | b;
                     continue;
                 }
                 // Primary colour optimization is in effect, we don't need
                 // to do any of the sillyness below just shift the value
                 // into the correct colour component slot and move on to
                 // the next pixel value.
                 if (colorOffset != 24)
                 {
                     intBuf[i] |= 0xFF000000;  // Alpha.
                     intBuf[i] |= discreteValue << colorOffset;
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
                     intBuf[i] = 0xFF000000 | newRValue << 16
                                | newGValue << 8 | newBValue;
                     continue;
                 }
                 // Add the existing colour component values to the new
                 // colour component values.
                 rValue = ((intBuf[i] & 0x00FF0000) >> 16) + newRValue;
                 gValue = ((intBuf[i] & 0x0000FF00) >> 8) + newGValue;
                 bValue = (intBuf[i] & 0x000000FF) + newBValue;

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
                 intBuf[i] = 0xFF000000 | rValue << 16 | gValue << 8 | bValue;
            }
        }

        // End the performance metrics for this rendering event.
        performanceStats.endRendering();
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

    /**
     * Retrieves the maximum number of reasonable tasks to schedule based on
     * image size and <i>maxTasks</i>.
     *
     * @param size The width along the X2 axis.
     * @return the number of tasks to schedule.
     */
    private int numTasks(int size) {
        for (int i = maxTasks; i > 0; i--) {
            if (size % i == 0) {
                return i;
            }
        }
        return 1;
    }

    /**
     * Creates a set of rendering tasks for the image based on the calling
     * buffer type.
     *
     * @param planeDef
     *            The plane to render.
     * @param buf
     *            The buffer to render into.
     * @return An array containing the tasks.
     * @throws IOException
     * @throws FormatException
     */
    private RenderingTask[] makeRenderingTasks(PlaneDef def, RGBBuffer buf) {
        List<RGBInterleavedRenderingTask> tasks = new ArrayList<RGBInterleavedRenderingTask>();

        //RenderingStats performanceStats = renderer.getStats();
        List<int[]> colors = getColors();
        List<LutReader> lutReaders = renderer.getLutProvider().getLutReaders(
                renderer.getChannelBindings());
        List<QuantumStrategy> strategies = getStrategies();
        // Create a number of rendering tasks.
        int taskCount = numTasks(sizeX2);
        int delta = sizeX2/taskCount;
        int x1Start = 0;
        int x1End = sizeX1;
        int x2Start, x2End;
        log.info("taskCount: "+taskCount+" delta: "+delta);
        BfPixelBuffer pixelBuffer = (BfPixelBuffer) renderer.getPixels();
        PixelData pixelData = getRawPixelData(def, pixelBuffer);
        for (int i = 0; i < taskCount; i++) {
            x2Start = i*delta;
            x2End = (i+1)*delta;
            tasks.add(new RGBInterleavedRenderingTask(buf, pixelData, strategies,
                    getChains(), colors, renderer.getOptimizations(),
                    x1Start, x1End, x2Start, x2End, lutReaders));
        }

        // Turn the list into an array an return it.
        return tasks.toArray(new RenderingTask[tasks.size()]);
    }

    private PixelData getRawPixelData(PlaneDef def, BfPixelBuffer pixelBuffer) {
        //TODO: Add performance stats?
        try
        {
            RegionDef region = def.getRegion();
            byte[] buf = new byte[region.getWidth() * region.getHeight()
                                  * FormatTools.getBytesPerPixel(PixelsType.VALUE_BIT) * 3];
            pixelBuffer.getTileDirect(0, 0, 0,
                    region.getX(),
                    region.getY(),
                    region.getWidth(),
                    region.getHeight(),
                    buf);

            ByteBuffer bbuf = ByteBuffer.wrap(buf);
            PixelData pixelData = new PixelData(renderer.getPixelsType().getValue(), bbuf);
            return pixelData;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally
        {
            // Make sure that the pixel buffer is cleansed properly.
            try
            {
                pixelBuffer.close();
            }
            catch (IOException e)
            {
                log.error("Pixels could not be closed successfully.", e);
                throw new ResourceError(
                        e.getMessage() + " Please check server log.");
            }
        }
    }

    private void render(RGBBuffer buf, PlaneDef planeDef) throws IOException,
        QuantizationException {
        RenderingStats performanceStats = renderer.getStats();
        // Process each active wavelength. If their number N > 1, then
        // process N-1 async and one in the current thread. If N = 1,
        // just use the current thread.
        RenderingTask[] tasks = makeRenderingTasks(planeDef, buf);
        performanceStats.startRendering();
        int n = tasks.length;
        Future[] rndTskFutures = new Future[n]; // [0] unused.
        ExecutorService processor = Executors.newCachedThreadPool();

        while (0 < --n) {
            rndTskFutures[n] = processor.submit(tasks[n]);
        }

        // Call the task in the current thread.
        if (n == 0) {
            tasks[0].call();
        }

        // Wait for all forked tasks (if any) to complete.
        for (n = 1; n < rndTskFutures.length; ++n) {
            try {
                rndTskFutures[n].get();
            } catch (Exception e) {
                if (e instanceof QuantizationException) {
                    throw (QuantizationException) e;
                }
                throw new RuntimeException(e);
            }
        }

        // Shutdown the task processor
        processor.shutdown();

        // End the performance metrics for this rendering event.
        performanceStats.endRendering();
    }

    @Override
    protected RGBAIntBuffer renderAsPackedIntAsRGBA(Renderer ctx, PlaneDef pd)
            throws IOException, QuantizationException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected int getImageSize(PlaneDef pd, Pixels pixels) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    protected String getPlaneDimsAsString(PlaneDef pd, Pixels pixels) {
        // TODO Auto-generated method stub
        return null;
    }

}
