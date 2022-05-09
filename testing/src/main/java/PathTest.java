import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;

public class PathTest {
    /**
     * The directory separator, a slash.
     */
    public static final String SEPARATOR = "/";

    /**
     * The directory separator, a slash, as a character.
     */
    public static final char SEPARATOR_CHAR = '/';

    /**
     * The current directory, ".".
     */
    public static final String CUR_DIR = ".";

    public static void main(String[] args) {
        String pathBig = "/Documents/School/College/MasonLeapLab_Research/ServerlessMDS/hops/testing/src/main/java/red/orange/yellow/green/blue/indigo/violet/PathTest.java";
        String pathModerate1 = "/home/ben/docs/research/serverlessMDS/eurosys22/";
        String pathModerate2 = "/home/ben/docs/research/serverlessMDS/eurosys22";
        String path = "/home/ben/docs/research/eval_plan.pdf";
        String pathRoot = "/";
        String pathBig2 = "/hops-dev-client_c62753fa-eeee-4b04-96f3-f8e92gaga3c7/hops_dir45/hops_dir46/hops_dir37/hops_file_4898";

        System.out.println(Arrays.toString(split(pathBig, SEPARATOR_CHAR)));
        System.out.println("\t" + getNumPathComponents(pathBig));
        System.out.println(Arrays.toString(split(pathModerate1, SEPARATOR_CHAR)));
        System.out.println("\t" + getNumPathComponents(pathModerate1));
        System.out.println(Arrays.toString(split(pathModerate2, SEPARATOR_CHAR)));
        System.out.println("\t" + getNumPathComponents(pathModerate2));
        System.out.println(Arrays.toString(split(path, SEPARATOR_CHAR)));
        System.out.println("\t" + getNumPathComponents(path));
        System.out.println(pathRoot + " (" + split(pathRoot, SEPARATOR_CHAR).length + ") : " + Arrays.toString(split(pathRoot, SEPARATOR_CHAR)));
        System.out.println("\t" + getNumPathComponents(pathRoot));
        System.out.println(pathBig2 + " (" + split(pathBig2, SEPARATOR_CHAR).length + ") : " + Arrays.toString(split(pathBig2, SEPARATOR_CHAR)));
        System.out.println("\t" + getNumPathComponents(pathBig2));

//        int numTrials = 1;
//
//        long start = System.currentTimeMillis();
//        for (int trial = 0; trial < numTrials; trial++) {
//            resolveRestOfThePathBytes(path);
//        }
//        long end = System.currentTimeMillis();
//
//        double durationMilliseconds = (end - start);
//        double avgDurationMilliseconds = durationMilliseconds / numTrials;
//        System.out.println("Results for BYTES");
//        System.out.println("\tAverage duration: " + avgDurationMilliseconds + " ms (n = " + numTrials + ").");
//        System.out.println("\tTotal duration of all trials: " + durationMilliseconds + " ms.");
//
//        start = System.currentTimeMillis();
//        for (int trial = 0; trial < numTrials; trial++) {
//            resolveRestOfThePathStrings(path);
//        }
//        end = System.currentTimeMillis();
//
//        durationMilliseconds = (end - start);
//        avgDurationMilliseconds = durationMilliseconds / numTrials;
//        System.out.println("Results for STRINGS");
//        System.out.println("\tAverage duration: " + avgDurationMilliseconds + " ms (n = " + numTrials + ").");
//        System.out.println("\tTotal duration of all trials: " + durationMilliseconds + " ms.");
    }

    public static int getNumPathComponents(String path) {
        if (path == null || !path.startsWith(SEPARATOR)) {
            throw new AssertionError("Absolute path required");
        }

        int pathComponents = 1;
        for (int i = 0; i < path.length(); i++) {
            if (path.charAt(i) == SEPARATOR_CHAR && i != (path.length() - 1))
                pathComponents++;
        }

        return pathComponents;
    }

    public static void resolveRestOfThePathBytes(String path) {
        byte[][] components = getPathComponents(path);
        String currentINode = "/";

        int i = 0;
        INodeBytesResolver resolver = new INodeBytesResolver(components);
        while (resolver.hasNext()) {
            currentINode = resolver.next();
            i += 1;
        }
    }

    public static void resolveRestOfThePathStrings(String path) {
        String[] components = getPathNames(path);
        String currentINode = "/";

        int i = 0;
        INodeStringsResolver resolver = new INodeStringsResolver(components);
        while (resolver.hasNext()) {
            currentINode = resolver.next();
            i += 1;
        }
    }

    public static String[] split(
            String str, char separator) {
        // String.split returns a single empty result for splitting the empty
        // string.
        if (str.isEmpty()) {
            return new String[]{""};
        }
        ArrayList<String> strList = new ArrayList<String>();
        int startIndex = 0;
        int nextIndex = 0;
        while ((nextIndex = str.indexOf(separator, startIndex)) != -1) {
            strList.add(str.substring(startIndex, nextIndex));
            startIndex = nextIndex + 1;
        }
        strList.add(str.substring(startIndex));
        // remove trailing empty split(s)
        int last = strList.size(); // last split
        while (--last>=0 && "".equals(strList.get(last))) {
            strList.remove(last);
        }
        return strList.toArray(new String[strList.size()]);
    }

    /**
     * Converts a string to a byte array using UTF8 encoding.
     */
    public static byte[] string2Bytes(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Breaks {@code path} into components.
     * @return array of byte arrays each of which represents
     * a single path component.
     */
    public static byte[][] getPathComponents(String path) {
        return getPathComponents(getPathNames(path));
    }

    /**
     * Convert strings to byte arrays for path components.
     */
    public static byte[][] getPathComponents(String[] strings) {
        if (strings.length == 0) {
            return new byte[][]{null};
        }
        byte[][] bytes = new byte[strings.length][];
        for (int i = 0; i < strings.length; i++) {
            bytes[i] = string2Bytes(strings[i]);
        }
        return bytes;
    }

    public static String[] getPathNames(String path) {
        if (path == null || !path.startsWith(SEPARATOR)) {
            throw new AssertionError("Absolute path required");
        }
        return StringUtils.split(path, SEPARATOR_CHAR);
    }
}