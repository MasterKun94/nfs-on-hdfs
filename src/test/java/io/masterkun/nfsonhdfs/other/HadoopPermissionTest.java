package io.masterkun.nfsonhdfs.other;

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;

public class HadoopPermissionTest {
    public static void main(String[] args) throws IOException {

        FsPermission permission = new FsPermission((short) 755);
        System.out.println(permission.toOctal());
        System.out.println(permission);
        System.out.println(permission.getUserAction().implies(FsAction.EXECUTE));

        FsPermission permission2 = FsPermission.getDefault();
        System.out.println(permission2.toOctal());
        System.out.println(permission2.toShort());
        System.out.println(permission2.toOctal() & 511);
        System.out.println(permission2.toShort() & 511);

        System.out.println(new FsPermission((short) (700 & 511)));
        System.out.println(new FsPermission((short) 511));

        int mode = 700;
        FsAction userAction = FsAction.values()[(mode / 100 % 10) & 7];
        FsAction groupAction = FsAction.values()[(mode / 10 % 10) & 7];
        FsAction otherAction = FsAction.values()[mode % 10 & 7];

        System.out.println(new FsPermission(userAction, groupAction, otherAction));
    }
}
