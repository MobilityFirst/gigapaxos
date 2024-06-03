package edu.umass.cs.xdn.experiment;

import edu.umass.cs.xdn.utils.Utils;

public class GetOSUserGroupID {

    public static void main(String[] args) {
        System.out.println("UID: " + Utils.getUid());
        System.out.println("GID: " + Utils.getGid());
    }
}
