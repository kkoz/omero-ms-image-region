package com.glencoesoftware.omero.ms.image.region;

import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.LoggerFactory;

import brave.ScopedSpan;
import brave.Tracing;
import io.vertx.core.json.JsonObject;
import omero.ApiUsageException;
import omero.ServerError;
import omero.api.IQueryPrx;
import omero.api.ServiceFactoryPrx;
import omero.model.Image;
import omero.model.Permissions;
import omero.sys.ParametersI;
import omero.util.IceMapper;

public class ZarrInitializerHandler {

    private static final org.slf4j.Logger log = LoggerFactory
            .getLogger(ZarrInitializerHandler.class);

    /** ZarrInitializerContext **/
    ZarrInitializerCtx zarrInitCtx;

    /** OMERO server pixels service. */
    private PixelsService pixelsService;

    /**
     * Mapper between <code>omero.model</code> client side Ice backed objects and
     * <code>ome.model</code> server side Hibernate backed objects.
     */
    protected final IceMapper mapper = new IceMapper();

    /**
     * Constructor
     * @param zarrInitCtx
     * @param pixelsService
     */
    public ZarrInitializerHandler(ZarrInitializerCtx zarrInitCtx,
            PixelsService pixelsService) {
        this.zarrInitCtx = zarrInitCtx;
        this.pixelsService = pixelsService;
    }



    public JsonObject initializeZarr(omero.client client) {
        ServiceFactoryPrx sf = client.getSession();
        try {
            Long imageId = zarrInitCtx.imageId;
            IQueryPrx iQuery = sf.getQueryService();
            Image image = queryImageData(iQuery, imageId);
            if (image == null) {
                return null;
            }
            //Check user permissions
            Permissions permissions = image.getDetails().getPermissions();
            if (!permissions.canEdit()) {
                log.error("User does not have permissions to edit image "
                        + Long.toString(image.getId().getValue()));
            }
            //Write JSON file
            StringBuilder sb = new StringBuilder();
            sb.append(pixelsService.getPixelsDirectory());
            sb.append(image.getId().getValue());
            sb.append("_zarr.json");
            log.info(sb.toString());
            File f = new File(sb.toString());
            if (f.exists()) {
                log.error("Zarr JSON file alreay exists for image "
                        + Long.toString(image.getId().getValue()));
                JsonObject retVal = new JsonObject();
                retVal.put("error", "Zarr JSON file already exists");
                return retVal;
            } else {
                f.createNewFile();
                FileWriter fw = new FileWriter(f);
                fw.write(zarrInitCtx.jsonData);
                fw.close();
                JsonObject retVal = new JsonObject();
                retVal.put("zarrJsonPath", sb.toString());
                return retVal;
            }
        } catch (Exception e) {
            log.error("Error initializing Zarr", e);
        }
        return null;
    }


    protected Image queryImageData(IQueryPrx iQuery, Long imageId)
            throws ApiUsageException, ServerError {
        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("query_image_data");
        try {
            Map<String, String> ctx = new HashMap<String, String>();
            ctx.put("omero.group", "-1");
            span.tag("omero.image_id", imageId.toString());
            ParametersI params = new ParametersI();
            params.addId(imageId);
            Image image = (Image) iQuery
                    .findByQuery("select i from Image as i "
                            + " join fetch i.pixels as p"
                            + " left outer JOIN FETCH i.datasetLinks as links "
                            + " left outer join fetch links.parent as dataset "
                            + " left outer join fetch dataset.projectLinks as plinks "
                            + " left outer join fetch plinks.parent as project "
                            + " join fetch i.details.owner as owner "
                            + " join fetch i.details.creationEvent "
                            + " where i.id=:id", params, ctx);
            return image;
        } finally {
            span.finish();
        }
    }


}
