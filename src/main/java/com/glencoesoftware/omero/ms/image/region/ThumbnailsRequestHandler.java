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

import static omero.rtypes.rint;
import static omero.rtypes.unwrap;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.slf4j.LoggerFactory;

import omero.ServerError;
import omero.api.ThumbnailStorePrx;
import omero.model.IObject;
import ome.model.core.Image;
import ome.model.core.Pixels;
import omero.sys.ParametersI;

import brave.ScopedSpan;
import brave.Tracing;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
/**
 * OMERO session aware handler whose event handler method conforms to the
 * {@link OmeroRequestHandler} interface. This class is expected to be used as
 * a lambda handler.
 * @author Chris Allan <callan@glencoesoftware.com>
 *
 */
public class ThumbnailsRequestHandler {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ThumbnailsRequestHandler.class);

    protected static final String GET_IMAGES = "omero.get_images";

    /** Longest side of the thumbnail. */
    protected final int longestSide;

    /** Image identifiers to request thumbnails for. */
    protected final List<Long> imageIds;

    protected final ThumbnailCtx thumbnailCtx;

    /** Handle on Vertx for event bus work*/
    private Vertx vertx;

    /**
     * Default constructor.
     * @param longestSide Size to confine or upscale the longest side of the
     * thumbnail to. The other side will then proportionately, based on aspect
     * ratio, be scaled accordingly.
     * @param imageIds {@link Image} identifiers to request thumbnails for.
     */
    public ThumbnailsRequestHandler(ThumbnailCtx thumbnailCtx,
            Vertx vertx,
            int longestSide,
            List<Long> imageIds) {
        this.thumbnailCtx = thumbnailCtx;
        this.vertx = vertx;
        this.longestSide = longestSide;
        this.imageIds = imageIds;
    }

    /**
     * Retrieves a map of JPEG thumbnails from the server.
     * @return CompletableFuture of a Map of {@link Image} identifier to JPEG thumbnail byte array.
     */
    public CompletableFuture<Map<Long, byte[]>> renderThumbnails() {
        CompletableFuture<Map<Long, byte[]>> promise = new CompletableFuture<Map<Long, byte[]>>();
        getImages(imageIds).whenComplete((images, t) -> {
            if (t != null) {
                promise.completeExceptionally(t);
                return;
            }l
            if (images.size() != 0) {
                promise.complete(getThumbnails(images, longestSide));
            } else {
                log.debug("Cannot find any Images with Ids {}", imageIds);
            }
        });
        return promise;
    }

    /**
     * Retrieves a list of loaded {@link Image}s from the server.
     * @param client OMERO client to use for querying.
     * @param imageIds {@link Image} identifiers to query for.
     * @return List of loaded {@link Image} and primary {@link Pixels}.
     * @throws ServerError If there was any sort of error retrieving the images.
     */
    protected CompletableFuture<List<Image>> getImages(List<Long> imageIds) {
        CompletableFuture<List<Image>> promise = new CompletableFuture<>();
        final JsonObject data = new JsonObject();
        data.put("sessionKey", thumbnailCtx.omeroSessionKey);
        final JsonArray jsonImageIds = new JsonArray(imageIds);
        data.put("imageIds", jsonImageIds);
        ScopedSpan span =
                Tracing.currentTracer().startScopedSpan("get_images");
        vertx.eventBus().<byte[]>request(
                GET_IMAGES, data, result -> {
            String s = "";
            try {
                if (result.failed()) {
                    span.finish();
                    promise.completeExceptionally(result.cause());
                    return;
                }

                byte[] body = result.result().body();
                ByteArrayInputStream bais = new ByteArrayInputStream(body);
                ObjectInputStream ois = new ObjectInputStream(bais);
                List<Image> images = (List<Image>) ois.readObject();
                promise.complete(images);
            } catch (IOException | ClassNotFoundException e) {
                log.error("Exception while decoding object in response", e);
                promise.completeExceptionally(e);
            } finally {
                span.finish();
            }
        });
        return promise;
    }

    /**
     * Retrieves a map of JPEG thumbnails from the server.
     * @param client OMERO client to use for thumbnail retrieval.
     * @param images {@link Image} list to retrieve thumbnails for.
     * @param longestSide Size to confine or upscale the longest side of each
     * thumbnail to. The other side will then proportionately, based on aspect
     * ratio, be scaled accordingly.
     * @return Map of {@link Image} identifier to JPEG thumbnail byte array.
     * @throws ServerError If there was any sort of error retrieving the
     * thumbnails.
     */
    protected Map<Long, byte[]> getThumbnails(List<Image> images, int longestSide){
        ScopedSpan span1 =
                Tracing.currentTracer().startScopedSpan("get_thumbnails");
        ThumbnailStorePrx thumbnailStore =
                client.getSession().createThumbnailStore();
        try {
            Map<String, String> ctx = new HashMap<String, String>();
            Map<Long, Long> pixelsIdImageIds = new HashMap<Long, Long>();
            for (ome.model.IObject o : images) {
                Image image = (Image) o;
                pixelsIdImageIds.put(
                    image.getPrimaryPixels().getId(),
                    image.getId()
                );
                // Assume all the groups are the same
                ctx.put(
                    "omero.group",
                    String.valueOf(unwrap(
                            image.getDetails().getGroup().getId()))
                );
            }
            ScopedSpan span2 =
                    Tracing.currentTracer().startScopedSpan("get_thumbnail_by_longest_side");
            try {
                Map<Long, byte[]> pixelsIdThumbnails =
                        thumbnailStore.getThumbnailByLongestSideSet(
                            rint(longestSide),
                            new ArrayList<Long>(pixelsIdImageIds.keySet()),
                            ctx
                        );
                Map<Long, byte[]> imageIdThumbnails =
                        new HashMap<Long, byte[]>();
                for (Entry<Long, byte[]> v : pixelsIdThumbnails.entrySet()) {
                    imageIdThumbnails.put(
                        pixelsIdImageIds.get(v.getKey()),
                        v.getValue()
                    );
                }
                return imageIdThumbnails;
            } finally {
                span2.finish();
            }
        } finally {
            thumbnailStore.close();
            span1.finish();
        }
    }

}
