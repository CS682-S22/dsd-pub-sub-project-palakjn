package controllers;

import models.File;

import java.util.HashMap;
import java.util.Map;

public class CacheManager {
    Map<String, File> topics;

    private CacheManager() {
        this.topics = new HashMap<>();
    }
}
