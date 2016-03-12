package uk.org.richardjarvis.loader;

/**
 * Created by rjarvis on 24/02/16.
 */
public class FileAssessor {

    public enum FileType {ARCHIVE, COMPRESSED, AUDIO, IMAGE, TEXT, UNKNOWN}


    public static boolean isArchive(String streamType) {
        return getType(streamType).equals(FileType.ARCHIVE);
    }

    public static boolean isCompressed(String streamType) {

        return getType(streamType).equals(FileType.COMPRESSED);
    }

    public static FileType getType(String streamType) {

        if ("application/x-tar,application/x-tika-unix-dump,application/java-archive,application/x-7z-compressed,application/x-archive,application/x-cpo,application/zip".contains(streamType))
            return FileType.ARCHIVE;

        if ("application/x-bzip,application/x-bzip2,application/gzip,application/x-gzip,application/x-xz".contains(streamType))
            return FileType.COMPRESSED;

        if ("text/plain,text/csv".contains(streamType))
            return FileType.TEXT;

        if ("audio/mpeg,audio/x-wav".contains(streamType))
            return FileType.AUDIO;

        return FileType.UNKNOWN;
    }

}
