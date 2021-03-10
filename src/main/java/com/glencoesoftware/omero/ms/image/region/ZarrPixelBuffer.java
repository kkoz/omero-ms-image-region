package com.glencoesoftware.omero.ms.image.region;

import java.awt.Dimension;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import ome.io.nio.DimensionsOutOfBoundsException;
import ome.io.nio.PixelBuffer;
import ome.model.core.Pixels;
import ome.util.PixelData;
import omero.model.WellSample;
import omero.model.WellSampleI;

public class ZarrPixelBuffer implements PixelBuffer {

    /** The Pixels object represented by this PixelBuffer */
    Pixels pixels;

    /** Top-level directory for NGFF files */
    String ngffDir;

    /** Fileset ID */
    Long filesetId;

    /** Requested resolution level */
    int resolutionLevel;

    /** Total number of resolution levels */
    int resolutionLevels = -1;

    /** For performing zarr operations */
    OmeroZarrUtils zarrUtils;

    /** Optional WellSample if image is part of a plate */
    Optional<WellSampleI> opWellSample;

    /**
     * Default constructor
     * @param pixels The Pixels object represented by this PixelBuffer
     * @param ngffDir Top-level directory for NGFF files
     * @param filesetId Fileset ID
     * @param zarrUtils For performing zarr operations
     */
    public ZarrPixelBuffer(Pixels pixels, String ngffDir, Long filesetId, OmeroZarrUtils zarrUtils,
            Optional<WellSampleI> opWellSample) {
        this.pixels = pixels;
        this.ngffDir = ngffDir;
        this.filesetId = filesetId;
        this.zarrUtils = zarrUtils;
        this.opWellSample = opWellSample;
        this.resolutionLevels = this.getResolutionLevels();
        this.resolutionLevel = this.resolutionLevels - 1;
        if (this.resolutionLevel < 0) {
            throw new IllegalArgumentException("This Zarr file has no pixel data");
        }
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void checkBounds(Integer x, Integer y, Integer z, Integer c, Integer t)
            throws DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub

    }

    @Override
    public Long getPlaneSize() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Integer getRowSize() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Integer getColSize() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Long getStackSize() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Long getTimepointSize() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Long getTotalSize() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Long getHypercubeSize(List<Integer> offset, List<Integer> size, List<Integer> step)
            throws DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Long getRowOffset(Integer y, Integer z, Integer c, Integer t) throws DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Long getPlaneOffset(Integer z, Integer c, Integer t) throws DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Long getStackOffset(Integer c, Integer t) throws DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Long getTimepointOffset(Integer t) throws DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PixelData getHypercube(List<Integer> offset, List<Integer> size, List<Integer> step)
            throws IOException, DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte[] getHypercubeDirect(List<Integer> offset, List<Integer> size, List<Integer> step, byte[] buffer)
            throws IOException, DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte[] getPlaneRegionDirect(Integer z, Integer c, Integer t, Integer count, Integer offset, byte[] buffer)
            throws IOException, DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PixelData getTile(Integer z, Integer c, Integer t, Integer x, Integer y, Integer w, Integer h)
            throws IOException {
        StringBuffer domStrBuf = new StringBuffer();
        domStrBuf.append("[")
            .append(t).append(",")
            .append(c).append(",")
            .append(z).append(",")
            .append(y).append(":").append(y + h).append(",")
            .append(x).append(":").append(x + w).append("]");
        return zarrUtils.getPixelData(ngffDir, filesetId, pixels.getImage().getSeries(), resolutionLevel,
                domStrBuf.toString(), opWellSample);
    }

    @Override
    public byte[] getTileDirect(Integer z, Integer c, Integer t, Integer x, Integer y, Integer w, Integer h,
            byte[] buffer) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PixelData getRegion(Integer size, Long offset) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte[] getRegionDirect(Integer size, Long offset, byte[] buffer) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PixelData getRow(Integer y, Integer z, Integer c, Integer t)
            throws IOException, DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PixelData getCol(Integer x, Integer z, Integer c, Integer t)
            throws IOException, DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte[] getRowDirect(Integer y, Integer z, Integer c, Integer t, byte[] buffer)
            throws IOException, DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte[] getColDirect(Integer x, Integer z, Integer c, Integer t, byte[] buffer)
            throws IOException, DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PixelData getPlane(Integer z, Integer c, Integer t) throws IOException, DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PixelData getPlaneRegion(Integer x, Integer y, Integer width, Integer height, Integer z, Integer c,
            Integer t, Integer stride) throws IOException, DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte[] getPlaneDirect(Integer z, Integer c, Integer t, byte[] buffer)
            throws IOException, DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PixelData getStack(Integer c, Integer t) throws IOException, DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte[] getStackDirect(Integer c, Integer t, byte[] buffer)
            throws IOException, DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PixelData getTimepoint(Integer t) throws IOException, DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte[] getTimepointDirect(Integer t, byte[] buffer) throws IOException, DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setTile(byte[] buffer, Integer z, Integer c, Integer t, Integer x, Integer y, Integer w, Integer h)
            throws IOException, BufferOverflowException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setRegion(Integer size, Long offset, byte[] buffer) throws IOException, BufferOverflowException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setRegion(Integer size, Long offset, ByteBuffer buffer) throws IOException, BufferOverflowException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setRow(ByteBuffer buffer, Integer y, Integer z, Integer c, Integer t)
            throws IOException, DimensionsOutOfBoundsException, BufferOverflowException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setPlane(ByteBuffer buffer, Integer z, Integer c, Integer t)
            throws IOException, DimensionsOutOfBoundsException, BufferOverflowException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setPlane(byte[] buffer, Integer z, Integer c, Integer t)
            throws IOException, DimensionsOutOfBoundsException, BufferOverflowException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setStack(ByteBuffer buffer, Integer z, Integer c, Integer t)
            throws IOException, DimensionsOutOfBoundsException, BufferOverflowException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setStack(byte[] buffer, Integer z, Integer c, Integer t)
            throws IOException, DimensionsOutOfBoundsException, BufferOverflowException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setTimepoint(ByteBuffer buffer, Integer t)
            throws IOException, DimensionsOutOfBoundsException, BufferOverflowException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setTimepoint(byte[] buffer, Integer t)
            throws IOException, DimensionsOutOfBoundsException, BufferOverflowException {
        // TODO Auto-generated method stub

    }

    @Override
    public byte[] calculateMessageDigest() throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getByteWidth() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean isSigned() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isFloat() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String getPath() {
        return Paths.get(ngffDir).resolve(Long.toString(pixels.getImage().getFileset().getId()) + ".tiledb")
                .resolve(Integer.toString(pixels.getImage().getSeries()))
                .resolve(Integer.toString(resolutionLevel)).toString();
    }

    @Override
    public long getId() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getSizeX() {
        return zarrUtils.getDimSize(ngffDir, filesetId, pixels.getImage().getSeries(), resolutionLevel, 4,
                opWellSample);
    }

    @Override
    public int getSizeY() {
        return zarrUtils.getDimSize(ngffDir, filesetId, pixels.getImage().getSeries(), resolutionLevel, 3,
                opWellSample);
    }

    @Override
    public int getSizeZ() {
        return zarrUtils.getDimSize(ngffDir, filesetId, pixels.getImage().getSeries(), resolutionLevel, 2,
                opWellSample);
    }

    @Override
    public int getSizeC() {
        return zarrUtils.getDimSize(ngffDir, filesetId, pixels.getImage().getSeries(), resolutionLevel, 1,
                opWellSample);
    }

    @Override
    public int getSizeT() {
        return zarrUtils.getDimSize(ngffDir, filesetId, pixels.getImage().getSeries(), resolutionLevel, 0,
                opWellSample);
    }

    @Override
    public int getResolutionLevels() {
        if (resolutionLevels < 0) {
        return zarrUtils.getResolutionLevels(ngffDir, filesetId, pixels.getImage().getSeries());
        } else {
            return resolutionLevels;
        }
    }

    @Override
    public int getResolutionLevel() {
        return Math.abs(
                resolutionLevel - (resolutionLevels - 1));
    }

    @Override
    public void setResolutionLevel(int resolutionLevel) {
        this.resolutionLevel = Math.abs(
                resolutionLevel - (resolutionLevels - 1));
    }

    @Override
    public Dimension getTileSize() {
        return new Dimension(getSizeX(), getSizeY());
    }

    @Override
    public List<List<Integer>> getResolutionDescriptions() {
        List<List<Integer>> resolutionDescriptions = new ArrayList<List<Integer>>();
        int originalResolution = resolutionLevel;
        for(int i = 0; i < resolutionLevels; i++) {
            this.resolutionLevel = i;
            List<Integer> description = new ArrayList<Integer>();
            Integer[] xy = zarrUtils.getSizeXandY(ngffDir, filesetId, pixels.getImage().getSeries(), resolutionLevel,
                    opWellSample);
            description.add(xy[0]);
            description.add(xy[1]);
            resolutionDescriptions.add(description);
        }
        setResolutionLevel(originalResolution);
        return resolutionDescriptions;
    }

}
