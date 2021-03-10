package com.glencoesoftware.omero.ms.image.region;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.slf4j.LoggerFactory;

import com.bc.zarr.DataType;
import com.bc.zarr.ZarrArray;
import com.bc.zarr.ZarrGroup;
import com.bc.zarr.ZarrUtils;

import brave.ScopedSpan;
import brave.Tracing;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import loci.formats.FormatTools;
import ome.util.PixelData;
import omero.model.Image;
import omero.model.WellSample;
import omero.model.WellSampleI;
import ucar.ma2.InvalidRangeException;

public class OmeroZarrUtils {

    private static final String MULTISCALES_KEY = "multiscales";
    private static final String MINMAX_KEY = "minmax";
    private static final String OMERO_KEY = "omero";
    private static final String LABELS = "labels";

    public static final String ZARR_EXTN = ".zarr";

    /** AWS/Cloud Access key */
    String accessKey;

    /** AWS/Cloud secret key */
    String secretKey;

    /** AWS/Cloud Region */
    String awsRegion;

    /** Cloud Endpoint Override */
    String s3EndpointOverride;

    /** Max Tile Length */
    Integer maxTileLength;

    /** S3 filesystem object */
    FileSystem s3fs;

    /**
     * Default constructor
     * @param accessKey AWS/Cloud Access Key
     * @param secretKey AWS/Cloud Secret Key
     * @param awsRegion AWS/Cloud Region
     * @param s3EndpointOverride For non-aws object storage endpoint
     * @param maxTileLength Max tile length
     * @param s3fsWrapper Configured S3 filesystem wrapper
     */
    public OmeroZarrUtils(String accessKey,
            String secretKey,
            String awsRegion,
            String s3EndpointOverride,
            Integer maxTileLength,
            S3FilesystemWrapper s3fsWrapper) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.awsRegion = awsRegion;
        this.s3EndpointOverride = s3EndpointOverride;
        this.maxTileLength = maxTileLength;
        this.s3fs = s3fsWrapper.getS3fs();
    }

    private static final org.slf4j.Logger log =
        LoggerFactory.getLogger(OmeroZarrUtils.class);

    /**
     * Get PixelType String from Zarr DataType
     * @param type The Zarr type
     * @return
     */
    public static String getPixelsType(DataType type) {
        switch (type) {
            case u1:
                return FormatTools.getPixelTypeString(FormatTools.UINT8);
            case i1:
                return FormatTools.getPixelTypeString(FormatTools.INT8);
            case u2:
                return FormatTools.getPixelTypeString(FormatTools.UINT16);
            case i2:
                return FormatTools.getPixelTypeString(FormatTools.INT16);
            case u4:
                return FormatTools.getPixelTypeString(FormatTools.UINT32);
            case i4:
                return FormatTools.getPixelTypeString(FormatTools.INT32);
            case f4:
                return FormatTools.getPixelTypeString(FormatTools.FLOAT);
            case f8:
                return FormatTools.getPixelTypeString(FormatTools.DOUBLE);
            default:
                throw new IllegalArgumentException("Attribute type " + type.toString() + " not supported");
        }
    }

    /**
     * Get PixelType String from Zarr DataType
     * @param type The Zarr type
     * @return
     */
    private static String getStdTypeFromZarrType(DataType zarrType) {
        switch (zarrType) {
        case u1:
            return FormatTools.getPixelTypeString(FormatTools.UINT8);
        case i1:
            return FormatTools.getPixelTypeString(FormatTools.INT8);
        case u2:
            return FormatTools.getPixelTypeString(FormatTools.UINT16);
        case i2:
            return FormatTools.getPixelTypeString(FormatTools.INT16);
        case u4:
            return FormatTools.getPixelTypeString(FormatTools.UINT32);
        case i4:
            return FormatTools.getPixelTypeString(FormatTools.INT32);
        case i8:
            return "int64";
        case f4:
            return "float";
        case f8:
            return "double";
        }
        return null;
    }

    /**
     * Get min and max values for metadata
     * @param buf Contains the data
     * @param type The Zarr DataType of the data in the ByteBuffer
     * @return long array [min, max]
     */
    public static long[] getMinMax(ByteBuffer buf, DataType type) {
        if (!buf.hasRemaining()) {
            throw new IllegalArgumentException("Cannot get max of empty buffer");
        }
        switch(type) {
            case u1: {
                long max = buf.get() & 0xff;
                long min = max;
                while(buf.hasRemaining()) {
                    long val = buf.get() & 0xff;
                    min = val < min ? val : min;
                    max = val > max ? val : max;
                }
                return new long[] {min, max};
            }
            case i1: {
                long max = buf.get();
                long min = max;
                while (buf.hasRemaining()) {
                    byte next = buf.get();
                    log.info(Byte.toString(next));
                    min = next < min ? next : min;
                    max = next > max ? next : max;
                }
                return new long[] {(long) min, (long) max};
            }
            case u2: {
                long max = buf.getShort() & 0xffff;
                long min = max;
                while(buf.hasRemaining()) {
                    long val = buf.getShort() & 0xffff;
                    min = val < min ? val : min;
                    max = val > max ? val : max;
                }
                return new long[] {min, max};
            }
            case i2: {
                long max = buf.getShort();
                long min = max;
                while (buf.hasRemaining()) {
                    short next = buf.getShort();
                    min = next < min ? next : min;
                    max = next > max ? next : max;
                }
                return new long[] {(long) min, (long) max};
            }
            case u4:{
                long max = buf.getInt() & 0xffffffffl;
                long min = max;
                while(buf.hasRemaining()) {
                    long val = buf.getInt() & 0xffffffffl;
                    min = val < min ? val : min;
                    max = val > max ? val : max;
                }
                return new long[] {min, max};
            }
            case i4: {
                long max = buf.getInt();
                long min = max;
                while (buf.hasRemaining()) {
                    int next = buf.getInt();
                    min = next < min ? next : min;
                    max = next > max ? next : max;
                }
                return new long[] {(long) min, (long) max};
            }
            default:
                throw new IllegalArgumentException("Type: " + type.toString() + " not supported");
        }
    }

    /**
     * Get bytes per pixel for a given DataType
     * @param type Zarr DataType
     * @return Number of bytes per pixel for the given DataType
     */
    public static int getBytesPerPixel(DataType type) {
        switch (type) {
            case u1:
            case i1:
                return 1;
            case u2:
            case i2:
                return 2;
            case u4:
            case i4:
                return 4;
            case i8:
                return 8;
            default:
                throw new IllegalArgumentException("Attribute type " + type.toString() + " not supported");
        }
    }

    /**
     * Get the region shape and the start (offset) from the string
     * @param domainStr The string which describes the domain
     * @return 2D int array [[shape_dim1,...],[start_dim1,...]]
     */
    public static int[][] getShapeAndStartFromString(String domainStr) {
        //String like [0,1,0,100:150,200:250]
        if(domainStr.length() == 0) {
            return null;
        }
        if(domainStr.startsWith("[")) {
            domainStr = domainStr.substring(1);
        }
        if(domainStr.endsWith("]")) {
            domainStr = domainStr.substring(0, domainStr.length() - 1);
        }
        String[] dimStrs = domainStr.split(",");
        if(dimStrs.length != 5) {
            throw new IllegalArgumentException("Invalid number of dimensions in domain string");
        }
        int[][] shapeAndStart = new int[][] {new int[5], new int[5]};
        for(int i = 0; i < 5; i++) {
            String s = dimStrs[i];
            if(s.contains(":")) {
                String[] startEnd = s.split(":");
                shapeAndStart[0][i] = Integer.valueOf(startEnd[1]) - Integer.valueOf(startEnd[0]); //shape
                shapeAndStart[1][i] = Integer.valueOf(startEnd[0]); //start
            } else {
                shapeAndStart[0][i] = 1; //shape - size 1 in this dim
                shapeAndStart[1][i] = Integer.valueOf(s); //start
            }
        }
        return shapeAndStart;
    }

    /**
     * Get the image data path from the constituent components
     * @param ngffDir Top-level directory containing NGFF files
     * @param filesetId FilesetID
     * @param series Series
     * @param resolutionLevel Requested resolution level
     * @return Path to the image pixel data
     * @throws IOException
     */
    public Path getImageDataPath(String ngffDir, Long filesetId, Integer series, Integer resolutionLevel,
            Optional<WellSampleI> opWellSample) throws IOException {
        Path imageDataPath = getLocalOrS3Path(ngffDir);
        imageDataPath = imageDataPath.resolve(Long.toString(filesetId)
                + ZARR_EXTN);
        if (opWellSample.isPresent()) {
            WellSample ws = opWellSample.get();
            String namingConvention = ws.getPlateAcquisition().getPlate().getColumnNamingConvention().getValue();
            int row = ws.getWell().getRow().getValue();
            int col = ws.getWell().getColumn().getValue();
            imageDataPath = imageDataPath.resolve(Integer.toString(row))
                    .resolve(Integer.toString(col))
                    .resolve(series.toString())
                    .resolve(resolutionLevel.toString());
        } else {
            imageDataPath = imageDataPath.resolve(Integer.toString(series)).resolve(Integer.toString(resolutionLevel));
        }
        return imageDataPath;
    }

    /**
     * Get the path to the label image data from constituent components
     * @param ngffDir Top-level directory containing NGFF files
     * @param filesetId Fileset ID
     * @param series Series
     * @param uuid Shape UUID
     * @param resolution Requested resolution level
     * @return Path to the reqeusted label image data
     * @throws IOException
     */
    private Path getLabelImagePath(String ngffDir, long filesetId, int series, String uuid, Integer resolution,
            Optional<WellSampleI> opWellSample) throws IOException {
        Path labelImageBasePath = getLocalOrS3Path(ngffDir);
        labelImageBasePath = labelImageBasePath.resolve(Long.toString(filesetId)
                + ZARR_EXTN);
        if (opWellSample.isPresent()) {
            WellSample ws = opWellSample.get();
            String namingConvention = ws.getPlateAcquisition().getPlate().getColumnNamingConvention().getValue();
            int row = ws.getWell().getRow().getValue();
            int col = ws.getWell().getColumn().getValue();
            labelImageBasePath = labelImageBasePath.resolve(Integer.toString(row))
                    .resolve(Integer.toString(col))
                    .resolve(Integer.toString(series))
                    .resolve(resolution.toString());
        } else {
            labelImageBasePath = labelImageBasePath.resolve(Integer.toString(series));
        }
        Path labelImageLabelsPath = labelImageBasePath.resolve(LABELS);
        Path labelImageShapePath = labelImageLabelsPath.resolve(uuid);
        Path fullNgffDir = labelImageShapePath.resolve(Integer.toString(resolution));
        return fullNgffDir;
    }

    /**
     * Get byte array of label image data
     * @param ngffDir Top-level directory containing NGFF files
     * @param filesetId Fileset ID
     * @param series Series
     * @param uuid Shape UUID
     * @param resolution Requested resolution
     * @param domainStr String like [0,1,0,100:150,200:250] denoting the region to return
     * @return The NGFF label image data
     */
    public byte[] getLabelImageBytes(String ngffDir, long filesetId, int series, String uuid, Integer resolution,
            String domainStr, Optional<WellSampleI> opWellSample) {
        ScopedSpan span = Tracing.currentTracer().startScopedSpan("get_label_image_bytes_zarr");
        try {
            Path ngffPath = getLabelImagePath(ngffDir, filesetId, series, uuid, resolution, opWellSample);
            ZarrArray zarray = ZarrArray.open(ngffPath);
            return OmeroZarrUtils.getData(zarray, domainStr, maxTileLength);
        } catch (IOException | InvalidRangeException e) {
            log.error("Failed to get label image bytes", e);
            return null;
        } finally {
            span.finish();
        }
    }

    /**
     * Get the NGFF image pixel data
     * @param ngffDir Top-level directory containing NGFF files
     * @param filesetId Fileset ID
     * @param series Series
     * @param resolutionLevel Requested resolution level
     * @param domainStr String like [0,1,0,100:150,200:250] denoting the region to return
     * @return
     * @throws IOException
     */
    public PixelData getPixelData(String ngffDir, Long filesetId, Integer series, Integer resolutionLevel,
            String domainStr, Optional<WellSampleI> opWellSample) throws IOException {
        ScopedSpan span = Tracing.currentTracer().startScopedSpan("get_pixel_data_from_zarr");
        Path ngffPath = getImageDataPath(ngffDir, filesetId, series, resolutionLevel, opWellSample);
        try {
            ZarrArray array = ZarrArray.open(ngffPath);
            byte[] buffer = OmeroZarrUtils.getData(array, domainStr, maxTileLength);
            PixelData d = new PixelData(getPixelsType(array.getDataType()), ByteBuffer.wrap(buffer));
            d.setOrder(ByteOrder.nativeOrder());
            return d;
        } catch (IOException | InvalidRangeException e) {
            log.error("Error getting Zarr pixel data",e);
            return null;
        } finally {
            span.finish();
        }
    }

    /**
     * Get image pixel data for entire image
     * @param ngffDir Top-level directory containing NGFF files
     * @param filesetId Fileset ID
     * @param series Series
     * @param resolutionLevel Requested resolution level
     * @return The pixel data for the image
     * @throws IOException
     */
    public PixelData getPixelData(String ngffDir, Long filesetId, Integer series, Integer resolutionLevel,
            Optional<WellSampleI> opWellSample) throws IOException {
        ScopedSpan span = Tracing.currentTracer().startScopedSpan("get_pixel_data_from_zarr");
        Path ngffPath = getImageDataPath(ngffDir, filesetId, series, resolutionLevel, opWellSample);
        try {
            ZarrArray array = ZarrArray.open(ngffPath);
            byte[] buffer = OmeroZarrUtils.getData(array);
            PixelData d = new PixelData(getPixelsType(array.getDataType()), ByteBuffer.wrap(buffer));
            d.setOrder(ByteOrder.nativeOrder());
            return d;
        } catch (IOException | InvalidRangeException e) {
            log.error("Error getting Zarr pixel data",e);
            return null;
        } finally {
            span.finish();
        }
    }

    /**
     * Get data from ZarrArray as byte[]
     * @param zarray The ZarrArray to get data from
     * @return A byte array of data in the ZarrArray
     * @throws IOException
     * @throws InvalidRangeException
     */
    public static byte[] getData(ZarrArray zarray) throws IOException, InvalidRangeException {
        ScopedSpan span = Tracing.currentTracer().startScopedSpan("get_entire_data_from_zarr");
        try {
            int[] shape = zarray.getShape();
            int num_dims = shape.length;
            if (num_dims != 5) {
                throw new IllegalArgumentException("Number of dimensions must be 5. Actual was: "
                        + Integer.toString(num_dims));
            }
            return OmeroZarrUtils.getBytes(zarray, zarray.getShape(), new int[] {0,0,0,0,0});
        } finally {
            span.finish();
        }
    }

    /**
     * Get data from subarray of the ZarrArray
     * @param zarray The ZarrArray to get data from
     * @param subarrayString string like [0,1,0,100:150,200:250] describing the region to retrieve
     * @param maxTileLength The max region size to return
     * @return
     * @throws IOException
     * @throws InvalidRangeException
     */
    public static byte[] getData(ZarrArray zarray, String subarrayString, Integer maxTileLength) throws IOException, InvalidRangeException {
        ScopedSpan span = Tracing.currentTracer().startScopedSpan("get_subregion_data_from_zarr");
        try {
            int[] shape = zarray.getShape();
            int num_dims = shape.length;
            if (num_dims != 5) {
                throw new IllegalArgumentException("Number of dimensions must be 5. Actual was: "
                        + Integer.toString(num_dims));
            }
            log.debug(subarrayString);
            int[][] shapeAndStart = getShapeAndStartFromString(subarrayString);
            if(shapeAndStart[0][3] > maxTileLength || shapeAndStart[0][4] > maxTileLength) {
                throw new IllegalArgumentException("Tile size exceeds max size of " + Integer.toString(maxTileLength));
            }
            return OmeroZarrUtils.getBytes(zarray, shapeAndStart[0], shapeAndStart[1]);
        } finally {
            span.finish();
        }
    }

    /**
     * Get byte array from ZarrArray
     * @param zarray The ZarrArray to get data from
     * @param shape The shape of the region to retrieve
     * @param offset The offset of the region
     * @return byte array of data from the ZarrArray
     */
    public static byte[] getBytes(ZarrArray zarray, int[] shape, int[] offset) {
        ScopedSpan span = Tracing.currentTracer().startScopedSpan("get_bytes_zarr");
        DataType type = zarray.getDataType();
        try {
            switch(type) {
                case u1:
                case i1:
                    return (byte[]) zarray.read(shape, offset);
                case u2:
                case i2:
                {
                    ScopedSpan readSpan = Tracing.currentTracer().startScopedSpan("zarr_read");
                    short[] data = (short[]) zarray.read(shape, offset);
                    readSpan.finish();
                    ByteBuffer bbuf = ByteBuffer.allocate(data.length * 2);
                    ShortBuffer sbuf = bbuf.asShortBuffer();
                    sbuf.put(data);
                    bbuf.order(ByteOrder.BIG_ENDIAN);
                    return bbuf.array();
                }
                case u4:
                case i4:
                {
                    ScopedSpan readSpan = Tracing.currentTracer().startScopedSpan("zarr_read");
                    int[] data = (int[]) zarray.read(shape, offset);
                    readSpan.finish();
                    ByteBuffer bbuf = ByteBuffer.allocate(data.length * 4);
                    IntBuffer ibuf = bbuf.asIntBuffer();
                    ibuf.put(data);
                    bbuf.order(ByteOrder.BIG_ENDIAN);
                    return bbuf.array();
                }
                case i8:
                {
                    ScopedSpan readSpan = Tracing.currentTracer().startScopedSpan("zarr_read");
                    long[] data = (long[]) zarray.read(shape, offset);
                    readSpan.finish();
                    ByteBuffer bbuf = ByteBuffer.allocate(data.length * 8);
                    LongBuffer lbuf = bbuf.asLongBuffer();
                    lbuf.put(data);
                    bbuf.order(ByteOrder.BIG_ENDIAN);
                    return bbuf.array();
                }
                case f4:
                case f8:
                default:
                    log.error("Unsupported data type" + type.toString());
                    return null;
            }
        } catch(InvalidRangeException|IOException e) {
            log.error("Error getting zarr PixelData", e);
            return null;
        } finally {
            span.finish();
        }
    }

    /**
     * Get the number of resolution levels in NGFF image data
     * @param ngffDir Top-level directory containing NGFF files
     * @param filesetId Fileset ID
     * @param series Series
     * @return Number of resolution levels
     */
    public int getResolutionLevels(String ngffDir, Long filesetId, Integer series) {
        ScopedSpan span = Tracing.currentTracer().startScopedSpan("get_resolution_levels_zarr");
        try {
            Path basePath;
            try {
                basePath = getLocalOrS3Path(ngffDir);
            } catch (IOException e) {
                log.error("Failed to get resolution levels form S3", e);
                return 0;
            }
            Path zarrSeriesPath = basePath.resolve(Long.toString(filesetId)
                    + ".zarr").resolve(Integer.toString(series));
            int count = 0;
            try {
                DirectoryStream<Path> stream = Files.newDirectoryStream(zarrSeriesPath);
                for (Path entry : stream) {
                    try {
                        Integer.parseInt(entry.getFileName().toString());
                        count++;
                    } catch (NumberFormatException e) {
                        continue;
                    }
                }
            } catch (IOException e) {
                log.error("Error counting resolution levels", e);
                span.error(e);
                return -1;
            }
            return count;
        } finally {
            span.finish();
        }
    }

    /**
     * Get the correct path (either local or cloud) to the NGFF files
     * @param ngffDir Top-level directory containing NGFF files
     * @return Path object to the NGFF directory
     * @throws IOException
     */
    public Path getLocalOrS3Path(String ngffDir) throws IOException {
        Path path;
        if(ngffDir.startsWith("s3://")) {
            if (s3fs == null) {
                throw new IOException("Cannot get s3 path from null FileSystem");
            }
            String s3BucketName = ngffDir.substring(5); // Remove s3://
            Path bucketPath = s3fs.getPath("/" + s3BucketName);
            return bucketPath;
        } else {
            path = Paths.get(ngffDir);
        }
        return path;
    }

    /**
     * Get the size of the image in the given dimension
     * @param ngffDir Top-level directory containing NGFF files
     * @param filesetId Fileset ID
     * @param series Series
     * @param resolutionLevel Requested resolution level
     * @param dimIdx The index of the dimension
     * @return Size of the requested dimension
     */
    public int getDimSize(String ngffDir, Long filesetId, Integer series, Integer resolutionLevel, Integer dimIdx,
            Optional<WellSampleI> opWellSample) {
        ScopedSpan span = Tracing.currentTracer().startScopedSpan("get_dim_size_zarr");
        try {
            Path imageDataPath = getImageDataPath(ngffDir, filesetId, series, resolutionLevel, opWellSample);
            ZarrArray zarray = ZarrArray.open(imageDataPath);
            return zarray.getShape()[dimIdx];
        } catch (IOException e) {
            log.error("Error while getting zarr dimension size", e);
        } finally {
            span.finish();
        }
        return -1;
    }

    /**
     * Get the X and Y sizes of the given image at the given resolution level
     * @param ngffDir Top-level directory containing NGFF files
     * @param filesetId Fileset ID
     * @param series Series
     * @param resolutionLevel Requested resolution level
     * @return Integer array [sizeX, sizeY]
     */
    public Integer[] getSizeXandY(String ngffDir, Long filesetId, Integer series, Integer resolutionLevel,
            Optional<WellSampleI> opWellSample) {
        ScopedSpan span = Tracing.currentTracer().startScopedSpan("get_size_xy_zarr");
        Integer[] xy = new Integer[2];
        try {
            ZarrArray zarray = ZarrArray.open(getImageDataPath(ngffDir, filesetId, series, resolutionLevel, opWellSample));
            xy[0] = zarray.getShape()[4];
            xy[1] = zarray.getShape()[3];
            return xy;
        } catch (IOException e) {
            log.error("Error in zarr getSizeXandY", e);
            return null;
        } finally {
            span.finish();
        }
    }

    /**
     * Get produce metadata JsonObject from ZarrArray info and other metadata info
     * @param zarray ZarrArray to get metadata from
     * @param minMax Min and max values of the data
     * @param multiscales Multiscales metadata object
     * @param uuid Shape uuid
     * @return Metadata JsonObject
     */
    private static JsonObject getMetadataFromArray(ZarrArray zarray, int[] minMax,
            JsonObject multiscales, String uuid) {
        ScopedSpan span = Tracing.currentTracer().startScopedSpan("get_metadata_from_array");
        int[] shape = zarray.getShape();

        JsonObject metadata = new JsonObject();
        if(minMax != null) {
            metadata.put("min", minMax[0]);
            metadata.put("max", minMax[1]);
        }
        JsonObject size = new JsonObject();
        size.put("t", shape[0]);
        size.put("c", shape[1]);
        size.put("z", shape[2]);
        size.put("height", shape[3]);
        size.put("width", shape[4]);

        metadata.put("size", size);
        metadata.put("type", getStdTypeFromZarrType(zarray.getDataType()));
        if(multiscales != null) {
            metadata.put(MULTISCALES_KEY, multiscales);
        }
        metadata.put("uuid", uuid);
        span.finish();
        return metadata;
    }

    /**
     * Get label image metadata request handler.
     * @param ngffDir Top-level directory containing NGFF files
     * @param filesetId The fileset ID of the image
     * @param series The series ID of the image in the fileset
     * @param uuid The External Info UUID of the shape associated with the label image
     * @param ngffDir the base directory for ngff data
     * @return A JsonObject with the label image metadata
     */
    public JsonObject getLabelImageMetadata(String ngffDir, long filesetId, int series, String uuid, int resolution) {
        log.info("Getting label image metadata from zarr in dir " + ngffDir);
        ScopedSpan span = Tracing.currentTracer().startScopedSpan("zarr_get_label_image_metadata");
        Path basePath;
        try {
            basePath = getLocalOrS3Path(ngffDir);
        } catch (IOException e) {
            log.error("Error getting metadata from s3", e);
            span.finish();
            return null;
        }
        Path labelImageBasePath = basePath.resolve(Long.toString(filesetId)
                + ".zarr").resolve(Integer.toString(series));
        Path labelImageLabelsPath = labelImageBasePath.resolve(LABELS);
        Path labelImageShapePath = labelImageLabelsPath.resolve(uuid);
        Path fullngffDir = labelImageShapePath.resolve(Integer.toString(resolution));
        JsonObject multiscales = null;
        int[] minMax = null;
        if (Files.exists(fullngffDir)) {
            try {
                ZarrGroup labelImageShapeGroup = ZarrGroup.open(labelImageShapePath);
                JsonObject jsonAttrs = new JsonObject(ZarrUtils.toJson(labelImageShapeGroup.getAttributes()));
                if (jsonAttrs.containsKey(MULTISCALES_KEY)) {
                    try {
                    multiscales = jsonAttrs.getJsonArray(MULTISCALES_KEY).getJsonObject(0);
                    } catch (Exception e) {
                        try {
                            log.warn("Failed to get multiscales as array - attempting as object");
                            multiscales = jsonAttrs.getJsonObject(MULTISCALES_KEY);
                        } catch (Exception e2) {
                            log.error("Failed to get multiscales metadata as array or object");
                        }
                    }
                    JsonArray datasets = multiscales.getJsonArray("datasets");
                    JsonArray resLvlArray = datasets;
                    for (int i = 0; i < resLvlArray.size(); i++) {
                        Path resPath = labelImageShapePath.resolve(Integer.toString(i));
                        ZarrArray za = ZarrArray.open(resPath);
                        JsonArray chunkArray = new JsonArray();
                        int[] chunks = za.getChunks();
                        for (int j = 0; j < chunks.length; j++) {
                            chunkArray.add(chunks[j]);
                        }
                        datasets.getJsonObject(i).put("chunksize", chunkArray);
                    }
                } if (jsonAttrs.containsKey(MINMAX_KEY)) {
                    JsonArray minMaxArray = jsonAttrs.getJsonArray(MINMAX_KEY);
                    minMax = new int[] {minMaxArray.getInteger(0), minMaxArray.getInteger(1)};
                }
            } catch (Exception e) {
                log.error("Exception while retrieving zarr label image metadata", e);
            }
            try {
                ZarrArray zarray = ZarrArray.open(fullngffDir);
                return getMetadataFromArray(zarray, minMax, multiscales, uuid);
            } catch (Exception e) {
                log.error("Exception while retrieving label image metadata", e);
            } finally {
                span.finish();
            }
        }
        span.finish();
        return null;
    }

    /**
     * Get a list of resolution descriptions (X and Y sizes)
     * @param ngffDir Top-level directory containing NGFF files
     * @param filesetId Fileset ID
     * @param series Series
     * @return List of Resolution level sizes
     */
    public List<List<Integer>> getResolutionDescriptions(String ngffDir, long filesetId, int series,
            Optional<WellSampleI> opWellSample) {
        List<List<Integer>> resolutionDescriptions = new ArrayList<List<Integer>>();
        int resLvlCount = getResolutionLevels(ngffDir, filesetId, series);
        for(int i = 0; i < resLvlCount; i++) {
            List<Integer> description = new ArrayList<Integer>();
            Integer[] xy = getSizeXandY(ngffDir, filesetId, series, i, opWellSample);
            description.add(xy[0]);
            description.add(xy[1]);
            resolutionDescriptions.add(description);
        }
        return resolutionDescriptions;
    }

    /**
     * Get 'omero' metadata for the image
     * @param ngffDir Top-level directory containing NGFF files
     * @param filesetId Fileset ID
     * @param series Series
     * @return JsonObject containing 'omero' metadata
     */
    public JsonObject getOmeroMetadata(String ngffDir, long filesetId, int series) {
        ScopedSpan span = Tracing.currentTracer().startScopedSpan("zarr_get_omero_metadata");
        Path basePath;
        try {
            basePath = getLocalOrS3Path(ngffDir);
        } catch (IOException e) {
            log.error("Error getting metadata from s3", e);
            span.error(e);
            span.finish();
            return null;
        }
        Path ngffPath = basePath.resolve(Long.toString(filesetId)
                + ".zarr").resolve(Integer.toString(series));
        try {
            ZarrGroup zarrGroup = ZarrGroup.open(ngffPath);
            JsonObject jsonAttrs = new JsonObject(ZarrUtils.toJson(zarrGroup.getAttributes()));
            if (!jsonAttrs.containsKey(OMERO_KEY)) {
                return null;
            }
            try {
                return jsonAttrs.getJsonObject(OMERO_KEY);
            } catch (Exception e) {
                log.debug("Getting omero metadata as string");
                return new JsonObject(jsonAttrs.getString(OMERO_KEY));
            }
        } catch (Exception e) {
            log.error("Error getting omero metadata from zarr");
            span.error(e);
            return null;
        } finally {
            span.finish();
        }
    }

}
