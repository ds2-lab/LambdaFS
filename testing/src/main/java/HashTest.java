public class HashTest {
    public static void main(String[] args) {
        int numTrials = 1_000_000;
        long parentId = 123;
        String localName = "file.txt";

        long start1 = System.currentTimeMillis();
        for (int i = 0; i < numTrials; i++) {
            int key = computeParentIdLocalNameHash(parentId, localName);
        }
        long end1 = System.currentTimeMillis();
        long duration1 = end1 - start1;

        long start2 = System.currentTimeMillis();
        for (int i = 0; i < numTrials; i++) {
            String key = parentId + localName;
        }
        long end2 = System.currentTimeMillis();
        long duration2 = end2 - start2;

        long start3 = System.currentTimeMillis();
        for (int i = 0; i < numTrials; i++) {
            StringBuilder builder = new StringBuilder();
            builder.append(parentId);
            builder.append(localName);
            String key = builder.toString();
        }
        long end3 = System.currentTimeMillis();
        long duration3 = end2 - start2;

        System.out.println("Avg hash duration: " + (duration1/numTrials) + " ms.");
        System.out.println("Total: " + duration1 + " ms.");

        System.out.println("Avg string literal concatenation duration: " + (duration2/numTrials) + " ms.");
        System.out.println("Total: " + duration2 + " ms.");

        System.out.println("Avg string builder duration: " + (duration3/numTrials) + " ms.");
        System.out.println("Total: " + duration3 + " ms.");
    }

    private static int computeParentIdLocalNameHash(long parentId, String localName) {
        return (int) (parentId ^ localName.hashCode());
    }
}
