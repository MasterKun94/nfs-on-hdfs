package io.masterkun.nfsonhdfs.idmapping;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import io.masterkun.nfsonhdfs.util.Constants;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Object2IntHashMap;
import org.dcache.nfs.status.BadOwnerException;
import org.dcache.nfs.v4.NfsIdMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public abstract class AbstractIdMapping implements NfsIdMapping {

    private static final Logger LOG = LoggerFactory.getLogger(FileIdMapping.class);
    private final long reloadInterval;
    private volatile Object2IntHashMap<String> userUidMap;
    private volatile Object2IntHashMap<String> groupGidMap;
    private volatile Int2ObjectHashMap<String> uidUserMap;
    private volatile Int2ObjectHashMap<String> gidGroupMap;
    private long reloadTime;

    protected AbstractIdMapping(long reloadInterval) {
        this.reloadInterval = reloadInterval;
    }

    protected void maybeReload() {
        long current = System.currentTimeMillis();
        if (current - reloadTime > reloadInterval) {
            synchronized (this) {
                if (current - reloadTime > reloadInterval) try {
                    reload();
                } catch (Exception e) {
                    LOG.error("Reload error", e);
                }
            }
        }
    }

    protected abstract void doReload(BiMap<String, Integer> userUidMap, BiMap<String, Integer> groupGidMap) throws Exception;

    public void reload() throws Exception {
        BiMap<String, Integer> userUidBiMap = HashBiMap.create();
        BiMap<String, Integer> groupGidBiMap = HashBiMap.create();
        doReload(userUidBiMap, groupGidBiMap);
        LOG.debug("{} Reload user uid mapping: {}", this, userUidBiMap);
        LOG.debug("{} Reload group gid mapping: {}", this, groupGidBiMap);
        Object2IntHashMap<String> userUidMap = new Object2IntHashMap<>(-1);
        Object2IntHashMap<String> groupGidMap = new Object2IntHashMap<>(-1);
        Int2ObjectHashMap<String> uidUserMap = new Int2ObjectHashMap<>();
        Int2ObjectHashMap<String> gidGroupMap = new Int2ObjectHashMap<>();
        for (Map.Entry<String, Integer> entry : userUidBiMap.entrySet()) {
            userUidMap.put(entry.getKey(), entry.getValue());
            uidUserMap.put(entry.getValue(), entry.getKey());
        }
        for (Map.Entry<String, Integer> entry : groupGidBiMap.entrySet()) {
            groupGidMap.put(entry.getKey(), entry.getValue());
            gidGroupMap.put(entry.getValue(), entry.getKey());
        }
        this.userUidMap = userUidMap;
        this.groupGidMap = groupGidMap;
        this.uidUserMap = uidUserMap;
        this.gidGroupMap = gidGroupMap;
        this.reloadTime = System.currentTimeMillis();
    }

    public boolean hasUserPrincipal(String principal) {
        maybeReload();
        return userUidMap.containsKey(principal);
    }

    public boolean hasGroupPrincipal(String principal) {
        maybeReload();
        return groupGidMap.containsKey(principal);
    }

    @Override
    public int principalToUid(String principal) throws BadOwnerException {
        maybeReload();
        int uid = userUidMap.getValue(principal);
        if (uid == -1) {
            LOG.debug("Unknown user {}", principal);
            throw new BadOwnerException("unknown user " + principal);
        }
        return uid;
    }

    @Override
    public int principalToGid(String principal) throws BadOwnerException {
        maybeReload();
        int gid = groupGidMap.getValue(principal);
        if (gid == -1) {
            LOG.debug("Unknown group {}", principal);
            throw new BadOwnerException("unknown group " + principal);
        }
        return gid;
    }

    @Override
    public String uidToPrincipal(int id) {
        maybeReload();
        String principal = uidUserMap.get(id);
        if (principal == null) {
            LOG.debug("Unknown uid {}", id);
            return Constants.NFS_NOBODY;
        }
        return principal;
    }

    @Override
    public String gidToPrincipal(int id) {
        maybeReload();
        String principal = gidGroupMap.get(id);
        if (principal == null) {
            LOG.debug("Unknown gid {}", id);
            return Constants.NFS_NOBODY;
        }
        return principal;
    }

    public abstract String toString();
}
