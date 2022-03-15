package models;

import java.util.ArrayList;
import java.util.List;

public class Segment {
    private List<Integer> offsets;

    public Segment() {
        this.offsets = new ArrayList<>();
    }

    public void addOffset(int offset) {
        offsets.add(offset);
    }

    public boolean isOffsetExist(int offset) {
        return offsets.contains(offset);
    }
}
