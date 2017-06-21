package org.apache.zeppelin.notebook.repo;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.appengine.tools.cloudstorage.GcsFileOptions;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsInputChannel;
import com.google.appengine.tools.cloudstorage.GcsOutputChannel;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.cloudstorage.ListItem;
import com.google.appengine.tools.cloudstorage.ListOptions;
import com.google.appengine.tools.cloudstorage.ListResult;
import com.google.appengine.tools.cloudstorage.RetryParams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.NoteInfo;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.scheduler.Job;
import org.apache.zeppelin.user.AuthenticationInfo;


public class AppEngineGcsNotebookRepo implements NotebookRepo {

    private final GcsService gcsService = GcsServiceFactory.createGcsService(new RetryParams.Builder()
            .initialRetryDelayMillis(10)
            .retryMaxAttempts(10)
            .totalRetryPeriodMillis(15000)
            .build());

    private static final int BUFFER_SIZE = 2 * 1024 * 1024;
    private final ZeppelinConfiguration conf;
    private String user;
    private String bucket;

    public AppEngineGcsNotebookRepo(ZeppelinConfiguration conf) {
        this.conf = conf;
        user = conf.getGcsUser();
        bucket = conf.getBucketName();
    }

    @Override
    public List<NoteInfo> list(AuthenticationInfo subject) throws IOException {
        ListOptions listOptions = new ListOptions.Builder().
                setPrefix(getNotebookDir()).setRecursive(false).build();
        ListResult listResult = gcsService.list(bucket, listOptions);
        List<NoteInfo> noteInfos = new ArrayList<>();
        while(listResult.hasNext()) {
            ListItem listItem = listResult.next();
            noteInfos.add(getNoteInfo(listItem.getName()));
        }
        return noteInfos;
    }

    private NoteInfo getNoteInfo(String objectName) throws IOException {
        return new NoteInfo(getNote(objectName));
    }

    @Override
    public Note get(String noteId, AuthenticationInfo subject) throws IOException {
        return getNote(getObjectName(noteId));
    }

    private Note getNote(String objectName) throws IOException {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.setPrettyPrinting();
        GcsInputChannel readChannel = gcsService.openPrefetchingReadChannel(
                new GcsFilename(bucket, objectName), 0, BUFFER_SIZE);
        try (InputStream ins = Channels.newInputStream(readChannel)) {
            String json = IOUtils.toString(ins, conf.getString(ZeppelinConfiguration.ConfVars.ZEPPELIN_ENCODING));
            Note note = Note.fromJson(json);
            for (Paragraph p : note.getParagraphs()) {
                if (p.getStatus() == Job.Status.PENDING || p.getStatus() == Job.Status.RUNNING) {
                    p.setStatus(Job.Status.ABORT);
                }
            }
            return note;
        }
    }

    @Override
    public void save(Note note, AuthenticationInfo subject) throws IOException {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.setPrettyPrinting();
        Gson gson = gsonBuilder.create();
        String json = gson.toJson(note);
        File tempFile = File.createTempFile("note", "json");
        try {
            FileUtils.writeStringToFile(tempFile, json);
            GcsFileOptions instance = GcsFileOptions.getDefaultInstance();
            GcsOutputChannel outputChannel = gcsService.createOrReplace(new GcsFilename(bucket, getObjectName(note.getId())), instance);
            FileUtils.copyFile(tempFile, Channels.newOutputStream(outputChannel));
        } finally {
            FileUtils.deleteQuietly(tempFile);
        }
    }

    private String getObjectName(String noteId) {
        return String.format("%s/%s/note.json", getNotebookDir(), noteId);
    }

    private String getNotebookDir() {
        return String.format("%s/notebook", user);
    }

    @Override
    public void remove(String noteId, AuthenticationInfo subject) throws IOException {
        gcsService.delete(new GcsFilename(bucket, getObjectName(noteId)));
    }

    @Override
    public void close() {

    }

    @Override
    public Revision checkpoint(String noteId, String checkpointMsg, AuthenticationInfo subject) throws IOException {
        return null;
    }

    @Override
    public Note get(String noteId, String revId, AuthenticationInfo subject) throws IOException {
        return null;
    }

    @Override
    public List<Revision> revisionHistory(String noteId, AuthenticationInfo subject) {
        return null;
    }

    @Override
    public Note setNoteRevision(String noteId, String revId, AuthenticationInfo subject) throws IOException {
        return null;
    }

    @Override
    public List<NotebookRepoSettingsInfo> getSettings(AuthenticationInfo subject) {
        return null;
    }

    @Override
    public void updateSettings(Map<String, String> settings, AuthenticationInfo subject) {

    }
}
