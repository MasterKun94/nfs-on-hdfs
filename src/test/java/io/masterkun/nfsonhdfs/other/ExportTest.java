package io.masterkun.nfsonhdfs.other;

import org.dcache.nfs.ExportFile;

import java.io.File;

public class ExportTest {
    public static void main(String[] args) throws Exception {
        ExportFile exportFile = new ExportFile(new File("src/test/resources/export"));
        exportFile.exports().forEach(System.out::println);
    }
}
