package io.masterkun.nfsonhdfs.cache;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import org.dcache.nfs.vfs.Stat;

import java.io.IOException;
import java.util.Objects;

public class StatHolder implements IdentifiedDataSerializable {

    private Stat stat;

    public StatHolder(Stat stat) {
        this.stat = stat;
    }

    public StatHolder() {
    }

    @Override
    public int getFactoryId() {
        return 131;
    }

    @Override
    public int getClassId() {
        return 2;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(stat.getDev());
        out.writeLong(stat.getIno());
        out.writeInt(stat.getMode());
        out.writeInt(stat.getNlink());
        out.writeInt(stat.getUid());
        out.writeInt(stat.getGid());
        out.writeInt(stat.getRdev());
        out.writeLong(stat.getSize());
        out.writeLong(stat.getGeneration());
        out.writeLong(stat.getATime());
        out.writeLong(stat.getMTime());
        out.writeLong(stat.getCTime());
        boolean exists = stat.isDefined(Stat.StatAttribute.BTIME);
        out.writeBoolean(exists);
        if (exists) out.writeLong(stat.getBTime());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        stat = new Stat();
        stat.setDev(in.readInt());
        stat.setIno(in.readLong());
        stat.setMode(in.readInt());
        stat.setNlink(in.readInt());
        stat.setUid(in.readInt());
        stat.setGid(in.readInt());
        stat.setRdev(in.readInt());
        stat.setSize(in.readLong());
        stat.setGeneration(in.readLong());
        stat.setATime(in.readLong());
        stat.setMTime(in.readLong());
        stat.setCTime(in.readLong());
        if (in.readBoolean()) stat.setBTime(in.readLong());
    }

    public Stat getStatNotCreate() {
        return stat;
    }

    public Stat newStat() {
        Stat stat = new Stat();
        stat.setDev(this.stat.getDev());
        stat.setIno(this.stat.getIno());
        stat.setMode(this.stat.getMode());
        stat.setNlink(this.stat.getNlink());
        stat.setRdev(this.stat.getRdev());
        stat.setSize(this.stat.getSize());
        stat.setGeneration(this.stat.getGeneration());
        stat.setATime(this.stat.getATime());
        stat.setMTime(this.stat.getMTime());
        stat.setCTime(this.stat.getCTime());
        stat.setUid(this.stat.getUid());
        stat.setGid(this.stat.getGid());
        if (stat.isDefined(Stat.StatAttribute.BTIME)) stat.setBTime(this.stat.getBTime());
        return stat;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatHolder that = (StatHolder) o;
        return Objects.equals(stat, that.stat);
    }

    @Override
    public int hashCode() {
        return stat.hashCode();
    }

    @Override
    public String toString() {
        return "StatHolder{" + stat + '}';
    }
}
