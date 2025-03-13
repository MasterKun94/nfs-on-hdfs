package io.masterkun.nfsonhdfs.cache;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Objects;

public class CacheKey implements IdentifiedDataSerializable {
    private long parent;
    private String name;

    public CacheKey(long parent, String name) {
        this.parent = parent;
        this.name = name;
    }

    public CacheKey() {
    }

    @Override
    public int getFactoryId() {
        return 131;
    }

    @Override
    public int getClassId() {
        return 1;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(parent);
        out.writeString(name);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        parent = in.readLong();
        name = in.readString();
    }

    public long getParent() {
        return this.parent;
    }

    public String getName() {
        return this.name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CacheKey cacheKey = (CacheKey) o;
        return parent == cacheKey.parent && Objects.equals(name, cacheKey.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parent, name);
    }

    @Override
    public String toString() {
        return "CacheKey{" +
                "parent=" + parent +
                ", name='" + name + '\'' +
                '}';
    }
}
