package utilities;

/**
 * An extension class for String to perform few string operations.
 * Current version has one defined function as of now. In the future versions, more functions can be added.
 * Reference: Same class I used in all the previous projects in CS601.
 *
 * @author Palak Jain
 * */

public class Strings {
    /**
     * Checks if a given string is null or empty
     * @param s string to compare
     * @return true if string is null or empty else false
     */
    public static boolean isNullOrEmpty(String s) {
        return s == null || s.isBlank() || s.isEmpty();
    }
}
