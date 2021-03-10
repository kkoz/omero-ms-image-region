/*
 * Copyright (C) 2017 Glencoe Software, Inc. All rights reserved.
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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Optional;

import org.slf4j.LoggerFactory;

import ome.api.IQuery;
import ome.io.nio.BackOff;
import ome.io.nio.FilePathResolver;
import ome.io.nio.PixelBuffer;
import ome.io.nio.TileSizes;
import ome.model.core.Pixels;
import omero.model.WellSample;
import omero.model.WellSampleI;
/**
 * Subclass which overrides series retrieval to avoid the need for
 * an injected {@link IQuery}.
 * @author Chris Allan <callan@glencoesoftware.com>
 *
 */
public class PixelsService extends ome.io.nio.PixelsService {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ImageRegionRequestHandler.class);

    public PixelsService(
            String path, long memoizerWait, FilePathResolver resolver,
            BackOff backOff, TileSizes sizes, IQuery iQuery) {
        super(path, true, new File(new File(path), "BioFormatsCache"), memoizerWait, resolver, backOff, sizes, iQuery);
        log.info("Using image region PixelsService");
    }

    @Override
    protected int getSeries(Pixels pixels) {
        return pixels.getImage().getSeries();
    }

    /**
     * Returns a pixel buffer for a given set of pixels. Either a proprietary
     * ROMIO pixel buffer or a specific pixel buffer implementation.
     * @param pixels Pixels set to retrieve a pixel buffer for.
     * @param write Whether or not to open the pixel buffer as read-write.
     * <code>true</code> opens as read-write, <code>false</code> opens as
     * read-only.
     * @return A pixel buffer instance.
     * @since OMERO-Beta4.3
     */
    public PixelBuffer getPixelBuffer(Pixels pixels, boolean write)
    {
        PixelBuffer pb = _getPixelBuffer(pixels, write);
        if (log.isDebugEnabled()) {
            log.debug(pb +" for " + pixels);
        }
        return pb;
    }

    /**
     * Returns a TiledbPixelBuffer for a given set of pixels.
     * @param pixels Pixels set to retrieve a pixel buffer for.
     * @param write Whether or not to open the pixel buffer as read-write.
     * <code>true</code> opens as read-write, <code>false</code> opens as
     * read-only.
     * @return A pixel buffer instance.
     * @since OMERO-Beta4.3
     */
    public PixelBuffer getTiledbPixelBuffer(Pixels pixels, String ngffDir, TiledbUtils tiledbUtils) {
        log.info("Creating TiledbPixelBuffer");
        return new TiledbPixelBuffer(pixels, ngffDir, pixels.getImage().getFileset().getId(), tiledbUtils);
    }

    /**
     * Returns a ZarrPixelBuffer for a given set of pixels.
     * @param pixels Pixels set to retrieve a pixel buffer for.
     * @param write Whether or not to open the pixel buffer as read-write.
     * <code>true</code> opens as read-write, <code>false</code> opens as
     * read-only.
     * @return A pixel buffer instance.
     * @since OMERO-Beta4.3
     */
    public PixelBuffer getZarrPixelBuffer(Pixels pixels, String ngffDir, OmeroZarrUtils zarrUtils,
            Optional<WellSampleI> opWellSample) {
        return new ZarrPixelBuffer(pixels, ngffDir, pixels.getImage().getFileset().getId(), zarrUtils, opWellSample);
    }

    public PixelBuffer getNgffPixelBuffer(Pixels pixels, String ngffDir, TiledbUtils tiledbUtils, OmeroZarrUtils zarrUtils,
            Optional<WellSampleI> opWellSample) {
        try {
            return new ZarrPixelBuffer(pixels, ngffDir, pixels.getImage().getFileset().getId(), zarrUtils,
                    opWellSample);
        } catch (Exception e) {
            return new TiledbPixelBuffer(pixels, ngffDir, pixels.getImage().getFileset().getId(), tiledbUtils);
        }
    }
}

