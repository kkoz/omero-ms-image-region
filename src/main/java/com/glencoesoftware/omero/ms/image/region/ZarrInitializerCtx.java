package com.glencoesoftware.omero.ms.image.region;

import org.slf4j.LoggerFactory;

import com.glencoesoftware.omero.ms.core.OmeroRequestCtx;

import io.vertx.core.MultiMap;
import io.vertx.core.json.JsonObject;

public class ZarrInitializerCtx extends OmeroRequestCtx {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ZarrInitializerCtx.class);

    /** Image ID to initialize */
    public Long filesetId;

    public String jsonData;

    public ZarrInitializerCtx() {};

    public ZarrInitializerCtx(MultiMap params, String omeroSessionKey, String jsonData) {
        this.omeroSessionKey = omeroSessionKey;
        this.filesetId = Long.parseLong(params.get("filesetId"));
        this.jsonData = jsonData;
    }
}
