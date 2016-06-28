package com.talentica.hungryhippos.filesystem;

import com.talentica.hungryhippos.filesystem.main.FileOperations;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class FileOperationsTest {


    private static final String HUNGRYHIPPOS = "HungryHippos";

    private static final String testDirectory = "NewTestDirecotry";

    private static final String fileName = "newTestFile";

    private String hhroot;

    @Before
    public void init() {
        hhroot = FileOperations.getHungryHipposRoot();
        File rootDirectory = new File(hhroot);
        if (!rootDirectory.exists()) {
            rootDirectory.mkdirs();
        }

    }

    @Test
    public void testGetDefaultFileSystem() {
        FileSystem fs = FileOperations.getDefaultFileSystem();
        assertNotNull(fs);
    }

    @Test
    public void testGetRoot() {
        String os = System.getProperty("os.name");
        String root = FileOperations.getRoot();
        assertNotNull(root);
        if (os.equalsIgnoreCase("Linux")) {
            assertEquals("/", root);
        }

    }

    @Test
    public void testGetHungryHipposRoot() {
        String root = FileOperations.getHungryHipposRoot();
        assertNotNull(root);
        assertTrue(root.contains(HUNGRYHIPPOS));
    }

    @Test
    public void testGetUserHome() {
        String userHome = FileOperations.getUserHome();
        assertNotNull(userHome);
    }

    @Test
    public void testGetUserDir() {
        String userDir = FileOperations.getUserHome();
        assertNotNull(userDir);
    }

    @Test
    public void testCreatePath() {
        String name = "test";
        Path path = FileOperations.createPath(name);
        assertNotNull(path);
        assertNotEquals(name, path);
    }

    @Test
    public void testSetPermission() {
        String permission = "rwx--x--x";
        Set<PosixFilePermission> set = FileOperations.setPermission(permission);
        assertNotNull(set);
        assertNotEquals(0, set.size());
    }

    /**
     * This test is to whether the method is throwing an exception when illegal argument is provided.
     */
    @Test
    public void testSetPermissionFail() {
        try {
            String permission = "rwx--x--";
            Set<PosixFilePermission> set = FileOperations.setPermission(permission);
            assertTrue(false);
        } catch (Exception e) {
            assertTrue(true);
        }

    }

    @Test
    public void testSetAttributes() {
        FileOperations.setAttributes("rwxr-x--x");
    }

    @Test
    public void testSetAttributesFail() {
        try {
            FileOperations.setAttributes("rwxr-x--");
            assertTrue(false);

        } catch (Exception e) {
            assertTrue(true);
        }

    }

    @Test
    public void testCreateDirectory() {
        File file = new File(testDirectory);
        if (file.exists()) {
            file.delete();
        }
        String permission = "rwx--x--x";
        Set<PosixFilePermission> set = PosixFilePermissions.fromString(permission);
        FileAttribute<Set<PosixFilePermission>> attr =
                PosixFilePermissions.asFileAttribute(set);
        boolean status = FileOperations.createDirectory(file.toPath(), attr);
        assertTrue(status);
        assertTrue(file.exists());
        if (file.exists()) {
            file.delete();
        }
    }

    @Test
    public void testCreateDirectoryFail() {
        boolean status = false;
        File file = new File(testDirectory);
        if (file.exists()) {
            file.delete();
        }
        try {

            String permission = "rwx--x--";
            Set<PosixFilePermission> set = PosixFilePermissions.fromString(permission);
            FileAttribute<Set<PosixFilePermission>> attr =
                    PosixFilePermissions.asFileAttribute(set);
            status = FileOperations.createDirectory(file.toPath(), attr);
            assertTrue(false);
        } catch (Exception e) {
            assertFalse(status);
            assertFalse(file.exists());
            assertTrue(true);
        }
        if (file.exists()) {
            file.delete();
        }
    }

    @Test
    public void testDeleteFileString() {

        Path path = FileOperations.createPath(fileName);

        File file = new File(path.toString());

        try {
            if (!file.exists()) {
                file.createNewFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        boolean status = FileOperations.deleteFile(fileName);
        assertTrue(status);
        assertFalse(file.exists());

        if (file.exists()) {
            file.delete();
        }

    }


    @Test
    public void testDeleteFileStringFail() {


        Path path = FileOperations.createPath("test/" + fileName);
        File parentDir = new File(hhroot + "/test");
        File file = new File(path.toString());
        try {
            if (!parentDir.exists()) {
                parentDir.mkdirs();
            }
            file.createNewFile();
            boolean status = FileOperations.deleteFile("test");
            assertFalse(status);
            assertTrue(file.exists());
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (file.exists()) {
            file.delete();
        }

        if (parentDir.exists()) {
            parentDir.delete();
        }

    }


    @Test
    public void testDeleteFilePath() {


        Path path = FileOperations.createPath(fileName);

        File file = new File(path.toString());


        try {
            if (!file.exists()) {
                file.createNewFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        boolean status = FileOperations.deleteFile(path);
        assertTrue(status);
        assertFalse(file.exists());

        if (file.exists()) {
            file.delete();
        }

    }

    @Test
    public void testDeleteFilePathFail() {

        Path path = FileOperations.createPath("test/" + fileName);
        File parentDir = new File(hhroot + "/test");
        File file = new File(path.toString());
        try {
            if (!parentDir.exists()) {
                parentDir.mkdirs();
            }
            file.createNewFile();
            boolean status = FileOperations.deleteFile(parentDir.toPath());
            assertFalse(status);
            assertTrue(file.exists());
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (file.exists()) {
            file.delete();
        }

        if (parentDir.exists()) {
            parentDir.delete();
        }
    }

    @Test
    public void testDeleteEverything() {


        Path path = FileOperations.createPath(fileName);

        File file = new File(path.toString());

        try {
            if (!file.exists()) {
                file.createNewFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        boolean status = FileOperations.deleteFile(path);
        assertTrue(status);
        assertFalse(file.exists());

        if (file.exists()) {
            file.delete();
        }

    }

    @Test
    public void testDeleteEverythingFail() {


        Path path = FileOperations.createPath("test/" + fileName);
        File parentDir = new File(hhroot + "/test");
        File file = new File(path.toString());
        try {
            if (!parentDir.exists()) {
                parentDir.mkdirs();
            }
            file.createNewFile();
            boolean status = FileOperations.deleteFile(parentDir.toPath());
            assertFalse(status);
            assertTrue(file.exists());
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (file.exists()) {
            file.delete();
        }

        if (parentDir.exists()) {
            parentDir.delete();
        }
    }

    @Test
    public void testListFilesInsideHHRoot() {
        FileOperations.getHungryHipposRoot();
        FileOperations.listFilesInsideHHRoot();
        assertTrue(true);
    }

    @Test
    public void testListFilesInsideHHRootFail() {
        try {
            FileOperations.listFilesInsideHHRoot();
            assertTrue(false);
        } catch (Exception e) {
            assertTrue(true);
        }
    }

    @Test
    public void testListFilesString() {


        Path path = FileOperations.createPath("test/" + fileName);
        File parentDir = new File(hhroot + "/test");
        File file = new File(path.toString());
        try {
            if (!parentDir.exists()) {
                parentDir.mkdirs();
            }
            file.createNewFile();
            List<Path> listOfFiles = FileOperations.listFiles("test");
            assertNotNull(listOfFiles);
            assertNotEquals(listOfFiles.size(), 0);
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (file.exists()) {
            file.delete();
        }

        if (parentDir.exists()) {
            parentDir.delete();
        }
    }


    @Test
    public void testListFilesStringFail() {


        Path path = FileOperations.createPath(fileName);

        File file = new File(path.toString());

        try {
            if (!file.exists()) {
                file.createNewFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        List<Path> listOfFiles = FileOperations.listFiles(fileName);
        assertNotNull(listOfFiles);
        assertEquals(listOfFiles.size(), 0);


        if (file.exists()) {
            file.delete();
        }

    }

    @Test
    public void testListFilesPath() {


        Path path = FileOperations.createPath("test/" + fileName);
        File parentDir = new File(hhroot + "/test");
        File file = new File(path.toString());
        try {
            if (!parentDir.exists()) {
                parentDir.mkdirs();
            }
            file.createNewFile();
            List<Path> listOfFiles = FileOperations.listFiles(parentDir.toPath());
            assertNotNull(listOfFiles);
            assertNotEquals(listOfFiles.size(), 0);
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (file.exists()) {
            file.delete();
        }

        if (parentDir.exists()) {
            parentDir.delete();
        }
    }

    @Test
    public void testListFilesPathFail() {


        Path path = FileOperations.createPath(fileName);

        File file = new File(path.toString());


        try {
            if (!file.exists()) {
                file.createNewFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        List<Path> listOfFiles = FileOperations.listFiles(path);
        assertNotNull(listOfFiles);
        assertEquals(listOfFiles.size(), 0);


        if (file.exists()) {
            file.delete();
        }

    }

    @Test
    public void testCreateFileStringFileAttributeOfSetOfPosixFilePermission() {


        Path path = FileOperations.createPath(fileName);

        File file = new File(path.toString());


        String permission = "rwx--x--x";
        Set<PosixFilePermission> set = PosixFilePermissions.fromString(permission);
        FileAttribute<Set<PosixFilePermission>> attr =
                PosixFilePermissions.asFileAttribute(set);
        FileOperations.createFile(fileName, attr);
        assertTrue(file.exists());

        if (file.exists()) {
            file.delete();
        }

    }

    @Test
    public void testCreateFileStringFileAttributeOfSetOfPosixFilePermissionFail() {


        Path path = FileOperations.createPath(fileName);

        File file = new File(path.toString());


        String permission = "rwx--x--x";
        Set<PosixFilePermission> set = PosixFilePermissions.fromString(permission);
        FileAttribute<Set<PosixFilePermission>> attr =
                PosixFilePermissions.asFileAttribute(set);
        try {
            FileOperations.createFile("SomeOtherDir/" + fileName, attr);
        } catch (Exception e) {
            assertFalse(file.exists());
        }
        if (file.exists()) {
            file.delete();
        }

    }

    @Test
    public void testCreateFilePathFileAttributeOfSetOfPosixFilePermission() {


        Path path = FileOperations.createPath(fileName);

        File file = new File(path.toString());


        String permission = "rwx--x--x";
        Set<PosixFilePermission> set = PosixFilePermissions.fromString(permission);
        FileAttribute<Set<PosixFilePermission>> attr =
                PosixFilePermissions.asFileAttribute(set);
        FileOperations.createFile(path, attr);
        assertTrue(file.exists());

        if (file.exists()) {
            file.delete();
        }
    }

    @Test
    public void testCreateFilePathFileAttributeOfSetOfPosixFilePermissionFail() {


        Path path = FileOperations.createPath(fileName);

        File file = new File(path.toString());


        String permission = "rwx--x--x";
        Set<PosixFilePermission> set = PosixFilePermissions.fromString(permission);
        FileAttribute<Set<PosixFilePermission>> attr =
                PosixFilePermissions.asFileAttribute(set);
        try {
            FileOperations.createFile((new File("SomeOtherDir/" + fileName)).toPath(), attr);
        } catch (Exception e) {
            assertFalse(file.exists());
        }
        if (file.exists()) {
            file.delete();
        }
    }

    @Test
    public void testCheckFileExistStringLinkOptionArray() {


        Path path = FileOperations.createPath(fileName);

        File file = new File(path.toString());

        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        boolean status = FileOperations.checkFileExist(fileName, null);
        assertTrue(status);
        status = FileOperations.checkFileExist("SomeOtherFile", null);
        assertFalse(status);
        if (file.exists()) {
            file.delete();
        }

    }

    @Test
    public void testCheckFileExistStringLinkOptionArrayFail() {
        fail("Not yet implemented");
    }

    @Test
    public void testCheckFileExistPathLinkOptionArray() {


        Path path = FileOperations.createPath(fileName);

        File file = new File(path.toString());

        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        boolean status = FileOperations.checkFileExist(path, null);
        assertTrue(status);
        status = FileOperations.checkFileExist("SomeOtherFile", null);
        assertFalse(status);
        if (file.exists()) {
            file.delete();
        }

    }

    @Test
    public void testCheckFileExistPathLinkOptionArrayFail() {
        fail("Not yet implemented");
    }

    @Test
    public void testIsRegularExecutableFile() {

        Path path = FileOperations.createPath(fileName);

        String permission = "rwx--x--x";
        Set<PosixFilePermission> set = PosixFilePermissions.fromString(permission);
        FileAttribute<Set<PosixFilePermission>> attr =
                PosixFilePermissions.asFileAttribute(set);
        try {
            if (!Files.exists(path)) {
                Files.createFile(path, attr);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        boolean status = FileOperations.isRegularExecutableFile(fileName);
        assertTrue(status);

        if (Files.exists(path)) {
            try {
                Files.delete(path);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


        permission = "rw---x--x";
        set = PosixFilePermissions.fromString(permission);
        attr =
                PosixFilePermissions.asFileAttribute(set);
        try {
            if (!Files.exists(path)) {
                Files.createFile(path, attr);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        status = FileOperations.isRegularExecutableFile(fileName);
        assertFalse(status);

        if (Files.exists(path)) {
            try {
                Files.delete(path);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    @Test
    public void testIsReadableString() {
        String hhroot = FileOperations.getHungryHipposRoot();
        Path path = FileOperations.createPath(fileName);
        File rootDirectory = new File(hhroot);

        if (!rootDirectory.exists()) {
            rootDirectory.mkdirs();
        }

        String permission = "rwx--x--x";
        Set<PosixFilePermission> set = PosixFilePermissions.fromString(permission);
        FileAttribute<Set<PosixFilePermission>> attr =
                PosixFilePermissions.asFileAttribute(set);
        try {
            if (!Files.exists(path)) {
                Files.createFile(path, attr);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        boolean status = FileOperations.isReadable(fileName);


        if (Files.exists(path)) {
            try {
                Files.delete(path);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        assertTrue(status);

        permission = "-wx--x--x";
        set = PosixFilePermissions.fromString(permission);
        attr =
                PosixFilePermissions.asFileAttribute(set);
        try {
            if (!Files.exists(path)) {
                Files.createFile(path, attr);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        status = FileOperations.isReadable(fileName);


        if (Files.exists(path)) {
            try {
                Files.delete(path);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        assertFalse(status);
    }

    @Test
    public void testIsWritableString() {


        Path path = FileOperations.createPath(fileName);


        String permission = "rwx--x--x";
        Set<PosixFilePermission> set = PosixFilePermissions.fromString(permission);
        FileAttribute<Set<PosixFilePermission>> attr =
                PosixFilePermissions.asFileAttribute(set);
        try {
            if (!Files.exists(path)) {
                Files.createFile(path, attr);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        boolean status = FileOperations.isWritable(fileName);


        if (Files.exists(path)) {
            try {
                Files.delete(path);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        assertTrue(status);

        permission = "r-x--x--x";
        set = PosixFilePermissions.fromString(permission);
        attr =
                PosixFilePermissions.asFileAttribute(set);
        try {
            if (!Files.exists(path)) {
                Files.createFile(path, attr);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        status = FileOperations.isWritable(fileName);


        if (Files.exists(path)) {
            try {
                Files.delete(path);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        assertFalse(status);
    }

    @Test
    public void testIsExecutableString() {

        Path path = FileOperations.createPath(fileName);


        String permission = "rwx--x--x";
        Set<PosixFilePermission> set = PosixFilePermissions.fromString(permission);
        FileAttribute<Set<PosixFilePermission>> attr =
                PosixFilePermissions.asFileAttribute(set);
        try {
            if (!Files.exists(path)) {
                Files.createFile(path, attr);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        boolean status = FileOperations.isExecutable(fileName);


        if (Files.exists(path)) {
            try {
                Files.delete(path);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        assertTrue(status);

        permission = "rw---x--x";
        set = PosixFilePermissions.fromString(permission);
        attr =
                PosixFilePermissions.asFileAttribute(set);
        try {
            if (!Files.exists(path)) {
                Files.createFile(path, attr);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        status = FileOperations.isExecutable(fileName);


        if (Files.exists(path)) {
            try {
                Files.delete(path);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        assertFalse(status);
    }

    @Test
    public void testIsReadablePath() {

        Path path = FileOperations.createPath(fileName);

        String permission = "rwx--x--x";
        Set<PosixFilePermission> set = PosixFilePermissions.fromString(permission);
        FileAttribute<Set<PosixFilePermission>> attr =
                PosixFilePermissions.asFileAttribute(set);
        try {
            if (!Files.exists(path)) {
                Files.createFile(path, attr);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        boolean status = FileOperations.isReadable(path);


        if (Files.exists(path)) {
            try {
                Files.delete(path);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        assertTrue(status);

        permission = "-wx--x--x";
        set = PosixFilePermissions.fromString(permission);
        attr =
                PosixFilePermissions.asFileAttribute(set);
        try {
            if (!Files.exists(path)) {
                Files.createFile(path, attr);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        status = FileOperations.isReadable(path);


        if (Files.exists(path)) {
            try {
                Files.delete(path);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        assertFalse(status);
    }

    @Test
    public void testIsWritablePath() {

        Path path = FileOperations.createPath(fileName);


        String permission = "rwx--x--x";
        Set<PosixFilePermission> set = PosixFilePermissions.fromString(permission);
        FileAttribute<Set<PosixFilePermission>> attr =
                PosixFilePermissions.asFileAttribute(set);
        try {
            if (!Files.exists(path)) {
                Files.createFile(path, attr);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        boolean status = FileOperations.isWritable(path);


        if (Files.exists(path)) {
            try {
                Files.delete(path);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        assertTrue(status);

        permission = "r-x--x--x";
        set = PosixFilePermissions.fromString(permission);
        attr = PosixFilePermissions.asFileAttribute(set);
        try {
            if (!Files.exists(path)) {
                Files.createFile(path, attr);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        status = FileOperations.isWritable(path);


        if (Files.exists(path)) {
            try {
                Files.delete(path);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        assertFalse(status);
    }

    @Test
    public void testIsExecutablePath() {


        Path path = FileOperations.createPath(fileName);

        String permission = "rwx--x--x";
        Set<PosixFilePermission> set = PosixFilePermissions.fromString(permission);
        FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute(set);
        try {
            if (!Files.exists(path)) {
                Files.createFile(path, attr);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        boolean status = FileOperations.isExecutable(path);


        if (Files.exists(path)) {
            try {
                Files.delete(path);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        assertTrue(status);

        permission = "rw---x--x";
        set = PosixFilePermissions.fromString(permission);
        attr = PosixFilePermissions.asFileAttribute(set);
        try {
            if (!Files.exists(path)) {
                Files.createFile(path, attr);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        status = FileOperations.isExecutable(path);


        if (Files.exists(path)) {
            try {
                Files.delete(path);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        assertFalse(status);
    }

    @Test
    public void testSize() {
        Path path = FileOperations.createPath(fileName);
        String permission = "rwx--x--x";
        Set<PosixFilePermission> set = PosixFilePermissions.fromString(permission);
        FileAttribute<Set<PosixFilePermission>> attr =
                PosixFilePermissions.asFileAttribute(set);
        try {
            if (!Files.exists(path)) {
                Files.createFile(path, attr);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


        long size = FileOperations.size(path);

        try {
            assertEquals(Files.size(path), size, 0.00000001);
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (Files.exists(path)) {
            try {
                Files.delete(path);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testIsDirectory() {
        fail("Not yet implemented");
    }

    @Test
    public void testIsRegularFile() {
        fail("Not yet implemented");
    }

    @Test
    public void testIsSymbolicLink() {
        fail("Not yet implemented");
    }

    @Test
    public void testIsHidden() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetLastModifiedTime() {
        fail("Not yet implemented");
    }

    @Test
    public void testSetLastModifiedTime() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetOwner() {
        fail("Not yet implemented");
    }

    @Test
    public void testSetOwner() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetAttribute() {
        fail("Not yet implemented");
    }


    @Test
    public void testCreateDirectoriesAndFileStringFileAttributeOfSetOfPosixFilePermission() {

        Path path = FileOperations.createPath("testdir/abcd/" + fileName);

        String permission = "rwx--x--x";
        Set<PosixFilePermission> set = PosixFilePermissions.fromString(permission);
        FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute(set);
        try {
            FileOperations.createDirectoriesAndFile("testdir/abcd/" + fileName, attr);
        } catch (IOException e) {
            e.printStackTrace();
        }
        boolean status = Files.exists(path);

        try {
            Files.delete(path);
            Files.delete(FileOperations.createPath("testdir/abcd"));
            Files.delete(FileOperations.createPath("testdir"));

        } catch (Exception e) {
            e.printStackTrace();
        }

        assertTrue(status);
    }

    @Test
    public void testCreateDirectoriesAndFileStringFileAttributeOfSetOfPosixFilePermissionFail() {
        boolean status = false;
        Path path = FileOperations.createPath("testdir/abcd/" + fileName);

        String permission = "r-x--x--x";
        Set<PosixFilePermission> set = PosixFilePermissions.fromString(permission);
        FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute(set);
        try {
            Files.createDirectories((new File(hhroot+"/textdir")).toPath(),attr);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {

            FileOperations.createDirectoriesAndFile("testdir/abcd/" + fileName, attr);

        } catch (IOException e) {
            status = Files.exists(path);
        }


        try {
            Files.deleteIfExists(path);
            Files.deleteIfExists(FileOperations.createPath("testdir/abcd"));
            Files.deleteIfExists(FileOperations.createPath("testdir"));

        } catch (Exception e) {
            e.printStackTrace();
        }
        assertFalse(status);
    }

    @Test
    public void testCreateDirectoriesAndFilePathFileAttributeOfSetOfPosixFilePermission() {

        Path path = FileOperations.createPath("testdir/abcd/" + fileName);

        String permission = "rwx--x--x";
        Set<PosixFilePermission> set = PosixFilePermissions.fromString(permission);
        FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute(set);
        try {
            FileOperations.createDirectoriesAndFile(path, attr);
        } catch (IOException e) {
            e.printStackTrace();
        }
        boolean status = Files.exists(path);

        try {
            Files.delete(path);
            Files.delete(FileOperations.createPath("testdir/abcd"));
            Files.delete(FileOperations.createPath("testdir"));

        } catch (Exception e) {
            e.printStackTrace();
        }

        assertTrue(status);
    }

    @Test
    public void testCreateDirectoriesAndFilePathFileAttributeOfSetOfPosixFilePermissionFail() {
        boolean status = false;
        Path path = FileOperations.createPath("testdir/abcd/" + fileName);

        String permission = "r-x--x--x";
        Set<PosixFilePermission> set = PosixFilePermissions.fromString(permission);
        FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute(set);
        try {
            Files.createDirectories((new File(hhroot+"/textdir")).toPath(),attr);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {

            FileOperations.createDirectoriesAndFile(path, attr);

        } catch (IOException e) {
            status = Files.exists(path);
        }


        try {
            Files.deleteIfExists(path);
            Files.deleteIfExists(FileOperations.createPath("testdir/abcd"));
            Files.deleteIfExists(FileOperations.createPath("testdir"));

        } catch (Exception e) {
            e.printStackTrace();
        }
        assertFalse(status);
    }
}
