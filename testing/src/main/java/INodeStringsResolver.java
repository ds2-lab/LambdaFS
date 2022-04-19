import java.io.UnsupportedEncodingException;
import java.util.NoSuchElementException;

public class INodeStringsResolver {
    private final String[] components;
    private String currentInode;
    private int count = 0;

    public INodeStringsResolver(String[] components) {
        this.components = components;
        this.currentInode = "/";
    }

    public boolean hasNext() {
        if (currentInode == null) {
            return false;
        }
        return count + 1 < components.length;
    }

    public String next() {
        if (!hasNext()) {
            throw new NoSuchElementException(
                    "Trying to read more components than available");
        }

        count++;

        currentInode = components[count];
        return currentInode;
    }

    /**
     * Decode a specific range of bytes of the given byte array to a string
     * using UTF8.
     *
     * @param bytes The bytes to be decoded into characters
     * @param offset The index of the first byte to decode
     * @param length The number of bytes to decode
     * @return The decoded string
     */
    private static String bytes2String(byte[] bytes, int offset, int length) {
        try {
            return new String(bytes, offset, length, "UTF8");
        } catch(UnsupportedEncodingException e) {
            assert false : "UTF8 encoding is not supported ";
        }
        return null;
    }

    /**
     * Converts a byte array to a string using UTF8 encoding.
     */
    public static String bytes2String(byte[] bytes) {
        return bytes2String(bytes, 0, bytes.length);
    }
}
