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

import java.io.IOException;

import loci.common.DataTools;
import loci.formats.FormatException;
import loci.formats.FormatTools;
import loci.formats.IFormatReader;
import loci.formats.ImageReader;
import loci.formats.ReaderWrapper;
import ome.io.bioformats.BfPixelBuffer;

public class BfPixelBufferPlus extends BfPixelBuffer {

    /**
     *
     */
    private static final long serialVersionUID = 1L;
    ImageReader rawReader;

    public BfPixelBufferPlus(String filePath, IFormatReader bfReader)
            throws IOException, FormatException {
        super(filePath, bfReader);
        while (bfReader instanceof ReaderWrapper) {
            bfReader = bfReader.getUnderlyingReaders()[0];
        }
        if (bfReader instanceof ImageReader) {
            rawReader = (ImageReader) bfReader;
            return;
        }
        for (IFormatReader reader : bfReader.getUnderlyingReaders()) {
            if (reader instanceof ImageReader) {
                rawReader = (ImageReader) reader;
            } else if (reader instanceof ReaderWrapper) {

            }
        }
        if (rawReader == null) {
            throw new RuntimeException("No ImageReader in reader chain");
        }
    }

    public byte[] getInterleavedTile(int x, int y, int w, int h) throws IOException {
        int c = 3; //TODO: Can this ever be something else?
        try {
            byte[] buf = new byte[w * h * FormatTools.getBytesPerPixel(reader().getPixelsType()) * c];
            return rawReader.openBytes(0, buf, x, y, w, h);
        } catch (FormatException e) {
            throw new RuntimeException(e);
        }
    }

    /*
     * This does NOT behave like the normal getTileDirect. It retrieves
     * the raw data for plane index 0. This is for use with interleaved
     * data where retrieving by channel is not efficient
     */
    @Override
    public byte[] getTileDirect(Integer z, Integer c, Integer t, Integer x,
            Integer y, Integer w, Integer h, byte[] buf) throws IOException {
        try {
            return rawReader.openBytes(0, buf, x, y, w, h);
        } catch (FormatException e) {
            throw new RuntimeException(e);
        }
    }

    /* @see IFormatReader#openBytes(int, byte[], int, int, int, int) */
    public byte[] openBytes(int no, byte[] buf, int x, int y, int w, int h)
      throws IOException, FormatException
    {
        byte[] lastImage = null;

        /** Index of last image opened. */
        int lastImageIndex = -1;

        /** Series of last image opened. */
        int lastImageSeries = -1;

        /** X index of last image opened. */
        int lastImageX = -1;

        /** Y index of last image opened. */
        int lastImageY = -1;

        /** Width of last image opened. */
        int lastImageWidth = -1;

        /** Height of last image opened. */
        int lastImageHeight = -1;

      if (rawReader.isRGB() && !rawReader.isIndexed()) {
        int c = getSizeC() / rawReader.getEffectiveSizeC();
        int source = 0;
        int series = 0;
        int bpp = 1;

        if (source != lastImageIndex || series != lastImageSeries ||
          x != lastImageX || y != lastImageY || w != lastImageWidth ||
          h != lastImageHeight)
        {
          int strips = 1;

          // check how big the original image is; if it's larger than the
          // available memory, we will need to split it into strips

          Runtime rt = Runtime.getRuntime();
          long availableMemory = rt.freeMemory();
          long planeSize = DataTools.safeMultiply64(w, h, bpp, c);

          if (availableMemory < planeSize || planeSize > Integer.MAX_VALUE) {
            strips = (int) Math.sqrt(h);
          }

          int stripHeight = h / strips;
          int lastStripHeight = stripHeight + (h - (stripHeight * strips));
          for (int i=0; i<strips; i++) {
            int currentStripHeight = (i == strips - 1 ? lastStripHeight : stripHeight);
            lastImage = rawReader.openBytes(source, x, y + i * stripHeight, w,
              currentStripHeight);
            lastImageIndex = source;
            lastImageSeries = series;
            lastImageX = x;
            lastImageY = y + i * stripHeight;
            lastImageWidth = w;
            lastImageHeight = i == strips - 1 ? lastStripHeight : stripHeight;
            System.arraycopy(lastImage, 0, buf, i * stripHeight * w * bpp * c,
                    w * currentStripHeight * bpp * c);
          }
        } else {
            System.arraycopy(lastImage, 0, buf, 0,
                    lastImage.length);
        }
        return buf;
      } else {
          throw new IllegalArgumentException("Using RGBInterleavedReader with non-interleaved data!");
      }
    }



}
