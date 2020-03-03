package io.siddhi.extension.io.file.util;

import org.json.JSONObject;

import java.io.File;
import java.util.Date;


/**
 * Class for providing server connectors.
 */
public class Util {
    public static String getFileHandlerEvent(final File file, String listeningFileUri, String status) {
        boolean listenerEventsURLValidated = false;
        //If the listeningUri is a fileUri
        if (listeningFileUri != null) {
            if (file.getAbsolutePath().equals(listeningFileUri)) {
                listenerEventsURLValidated = true;
            }
        } else {
            // If the listeningUri is a directoryUri
            listenerEventsURLValidated = true;
        }
        if (listenerEventsURLValidated) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("filepath", file.getAbsolutePath());
            jsonObject.put("length", file.length());
            jsonObject.put("last_modified", new Date(file.lastModified()));
            jsonObject.put("status", status);
            return jsonObject.toString();
        } else {
            return null;
        }
    }
}
