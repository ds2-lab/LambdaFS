package io.hops.metadata.ndb.dalimpl.hdfs;

import java.util.ArrayList;

/**
 *
 * @author salman
 */
public class PathUtils {
    public static final String SEPARATOR = "/";
    public static final char SEPARATOR_CHAR = '/';

    public static String[] getPathNames(String path) {
        if (path == null || !path.startsWith(SEPARATOR)) {
            throw new AssertionError("Absolute path required");
        }
        return split(path, SEPARATOR_CHAR);
    }

    public static String[] split(
            String str, char separator) {
        // String.split returns a single empty result for splitting the empty
        // string.
        if (str.isEmpty()) {
            return new String[]{""};
        }
        ArrayList<String> strList = new ArrayList<>();
        int startIndex = 0;
        int nextIndex = 0;
        while ((nextIndex = str.indexOf(separator, startIndex)) != -1) {
            strList.add(str.substring(startIndex, nextIndex));
            startIndex = nextIndex + 1;
        }
        strList.add(str.substring(startIndex));
        // remove trailing empty split(s)
        int last = strList.size(); // last split
        while (--last >= 0 && "".equals(strList.get(last))) {
            strList.remove(last);
        }
        return strList.toArray(new String[strList.size()]);
    }
}
