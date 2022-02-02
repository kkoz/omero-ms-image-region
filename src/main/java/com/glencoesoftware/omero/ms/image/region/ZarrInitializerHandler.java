package com.glencoesoftware.omero.ms.image.region;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.crypto.Cipher;

import org.slf4j.LoggerFactory;

import brave.ScopedSpan;
import brave.Tracing;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import omero.ApiUsageException;
import omero.ServerError;
import omero.api.IQueryPrx;
import omero.api.ServiceFactoryPrx;
import omero.model.IObject;
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
            Long filesetId = zarrInitCtx.filesetId;
            JsonObject jsonData = new JsonObject(zarrInitCtx.jsonData);
            IQueryPrx iQuery = sf.getQueryService();
            List<Image> filesetImages = queryFilesetImages(iQuery, filesetId);
            if (filesetImages.isEmpty()) {
                return null;
            }
            JsonObject retVal = new JsonObject();
            JsonArray zarrPaths = new JsonArray();
            JsonArray errors = new JsonArray();
            for(Image image : filesetImages) {
                //Check user permissions
                Permissions permissions = image.getDetails().getPermissions();
                if (!permissions.canEdit()) {
                    log.error("User does not have permissions to edit image "
                            + Long.toString(image.getId().getValue()));
                }
                //Write JSON file
                StringBuilder sb = new StringBuilder();
                String pixPath = pixelsService.getPixelsPath(
                        image.getPrimaryPixels().getId().getValue());
                log.info(pixPath);
                sb.append(pixPath);
                sb.append("_zarr.json");
                log.info(sb.toString());
                File f = new File(sb.toString());
                Boolean overwrite = jsonData.getBoolean("overwrite", false);
                if (f.exists() && !overwrite) {
                    log.error("Zarr JSON file alreay exists for image "
                            + Long.toString(image.getId().getValue()));
                    errors.add(String.format(
                            "Zarr JSON file already exists for image %d",
                            image.getId().getValue()));
                } else {
                    JsonObject dataToWrite = new JsonObject();
                    String zarrPath = jsonData.getString("zarrPath");
                    if (!zarrPath.endsWith("/")) {
                        zarrPath = zarrPath + "/";
                    }
                    zarrPath = zarrPath + Integer.toString(image.getSeries().getValue());
                    if (jsonData.containsKey("encrypted") && jsonData.getBoolean("encrypted")) {
                        try {
                            byte[] fromFile = Files.readAllBytes(Paths.get("/OMERO56/Pixels/keys/public.der"));

                            X509EncodedKeySpec keySpec = new X509EncodedKeySpec(fromFile);
                            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
                            PublicKey rsaPubKey = keyFactory.generatePublic(keySpec);
                            Cipher encryptCipher = Cipher.getInstance("RSA/ECB/OAEPWithSHA1AndMGF1Padding");
                            encryptCipher.init(Cipher.ENCRYPT_MODE, rsaPubKey);
                            byte[] encryptedZarrPath = encryptCipher.doFinal(zarrPath.getBytes(StandardCharsets.UTF_8));
                            String encryptedString = Base64.getEncoder().encodeToString(encryptedZarrPath);
                            dataToWrite.put("zarrPath", encryptedString);
                        } catch (Exception e) {
                            log.error("Failed to encrypt zarrPath", e);
                            errors.add(String.format(
                                    "Failed to encrypt zarrPath for image %d",
                                    image.getId().getValue()));
                        }
                    }
                    else {
                        dataToWrite.put("zarrPath", zarrPath);
                    }
                    pixelsService.createParentDirs(pixPath);
                    f.createNewFile();
                    FileWriter fw = new FileWriter(f);
                    fw.write(dataToWrite.toString());
                    fw.close();
                    zarrPaths.add(sb.toString());
                }
            }
            if (zarrPaths.size() > 0) {
                retVal.put("zarrPaths", zarrPaths);
            }
            if (errors.size() > 0) {
                retVal.put("errors", errors);
            }
            return retVal;
        } catch (Exception e) {
            log.error("Error initializing Zarr", e);
        }
        return null;
    }


    protected List<Image> queryFilesetImages(IQueryPrx iQuery, Long filesetId)
            throws ApiUsageException, ServerError {
        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("query_image_data");
        try {
            Map<String, String> ctx = new HashMap<String, String>();
            ctx.put("omero.group", "-1");
            span.tag("omero.fileset_id", filesetId.toString());
            ParametersI params = new ParametersI();
            params.addId(filesetId);
            List<IObject> imageObjs = iQuery
                    .findAllByQuery("select i from Image as i "
                            + " join fetch i.fileset as f"
                            + " join fetch i.pixels as p"
                            + " join fetch i.details.owner as owner "
                            + " join fetch i.details.creationEvent "
                            + " where f.id=:id", params, ctx);
            return imageObjs.stream().map(Image.class::cast).collect(Collectors.toList());
        } finally {
            span.finish();
        }
    }


}
