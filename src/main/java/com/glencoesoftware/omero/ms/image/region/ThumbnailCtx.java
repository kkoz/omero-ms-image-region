package com.glencoesoftware.omero.ms.image.region;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.LoggerFactory;

import com.glencoesoftware.omero.ms.core.OmeroRequestCtx;

import io.vertx.core.MultiMap;

public class ThumbnailCtx extends OmeroRequestCtx {


    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ThumbnailCtx.class);

    /** The size of the longest side of the thumbnail */
    public Integer longestSide;

    /** Image ID */
    public Long imageId;

    /** Image IDs" */
    public List<Long> imageIds;

    /** Rendering Definition ID */
    public Long renderingDefId;

    /** Channel settings - handled at the Verticle level*/
    public List<Integer> channels;
    public List<Float[]> windows;
    public List<String> colors;

    /**
     * Constructor for jackson to decode the object from string
     */
    ThumbnailCtx() {};

    /**
     * Default constructor.
     * @param params {@link io.vertx.core.http.HttpServerRequest} parameters
     * required for rendering an image region.
     * @param omeroSessionKey OMERO session key.
     */
    ThumbnailCtx(MultiMap params, String omeroSessionKey) {
        this.omeroSessionKey = omeroSessionKey;

        this.longestSide = Optional.ofNullable(params.get("longestSide"))
                .map(Integer::parseInt)
                .orElse(96);

        this.imageIds = params.getAll("imageId").stream()
                .map(Long::parseLong)
                .collect(Collectors.toList());

        this.imageId = Optional.ofNullable(params.get("imageId"))
                .map(Long::parseLong)
                .orElse(null);

        this.getChannelInfoFromString("1|0:211$FF0000,2|0:211$00FF00,3|0:211$0000FF");

        /*
        this.renderingDefId = Optional.ofNullable(params.get("rdefId"))
        .map(Long::parseLong).orElse(null);
        */

    }

    /**
     * Parses a string to channel rendering settings.
     * Populates channels, windows and colors lists.
     * @param channelInfo string describing the channel rendering settings:
     * "-1|0:65535$0000FF,2|1755:51199$00FF00,3|3218:26623$FF0000"
     */
    private void getChannelInfoFromString(String channelInfo) {
        if (channelInfo == null) {
            return;
        }
        String[] channelArray = channelInfo.split(",", -1);
        channels = new ArrayList<Integer>();
        windows = new ArrayList<Float[]>();
        colors = new ArrayList<String>();
        for (String channel : channelArray) {
            try {
                // chan  1|12:1386r$0000FF
                // temp ['1', '12:1386r$0000FF']
                String[] temp = channel.split("\\|", 2);
                String active = temp[0];
                String color = null;
                Float[] range = new Float[2];
                String window = null;
                // temp = '1'
                // Not normally used...
                if (active.indexOf("$") >= 0) {
                    String[] split = active.split("\\$", -1);
                    active = split[0];
                    color = split[1];
                }
                channels.add(Integer.parseInt(active));
                if (temp.length > 1) {
                    if (temp[1].indexOf("$") >= 0) {
                        window = temp[1].split("\\$")[0];
                        color = temp[1].split("\\$")[1];
                    }
                    String[] rangeStr = window.split(":");
                    if (rangeStr.length > 1) {
                        range[0] = Float.parseFloat(rangeStr[0]);
                        range[1] = Float.parseFloat(rangeStr[1]);
                    }
                }
                colors.add(color);
                windows.add(range);
                log.debug("Adding channel: {}, color: {}, window: {}",
                        active, color, window);
            } catch (Exception e)  {
                throw new IllegalArgumentException("Failed to parse channel '"
                    + channel + "'");
            }
        }
    }
}
