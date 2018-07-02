/*
 * *****************************************************************************
 *   Copyright 2017 Talentica Software Pvt. Ltd.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *  *****************************************************************************
 */

package com.talentica.hungryHippos.rdd;

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;

import java.io.*;
import java.net.URI;
import java.util.StringTokenizer;

public class HHFileSystem extends FileSystem {
    private URI uri = URI.create("hhfs:///");
    private Path workingDir;
    private HHFSClient client;
    public static final String connectString = "hungry-hippos-connect-ip";


    @Override
    public URI getUri() {
        return uri;
    }

    @Override
    public String getScheme() {
        return "hhfs";
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        super.initialize(uri, conf);
        setConf(conf);
        String connectString = conf.get(HHFileSystem.connectString, "");
        HungryHippoCurator hungryHippoCurator = HungryHippoCurator.getInstance(connectString, 10000);
        File uriFile = new File(uri.getPath());
        this.client = new HHFSClient();
        this.uri = uri;
        this.workingDir = new Path(client.getTmpDir().getPath());
    }

    /**
     * Convert a path to a File.
     */
    public File pathToFile(Path path) {
        checkPath(path);
        if (!path.isAbsolute()) {
            path = new Path(getWorkingDirectory(), path);
        }
        return new File(path.toUri().getPath());
    }


    /*******************************************************
     * For open()'s FSInputStream.
     *******************************************************/
    class LocalFSFileInputStream extends FSInputStream implements HasFileDescriptor {
        private FileInputStream fis;
        private BufferedInputStream bis;
        private File file;
        private long position;
        private int bufferSize;


        public LocalFSFileInputStream(File file, int bufferSize) throws IOException {
            fis = new FileInputStream(file);
            bis = new BufferedInputStream(fis, bufferSize);
            this.file = file;
            this.bufferSize = bufferSize;
        }

        @Override
        public void seek(long targetPos) throws IOException {
            if (targetPos < 0) {
                throw new EOFException(
                        FSExceptionMessages.NEGATIVE_SEEK);
            }
            if (position == targetPos) return;
            bis.close();
            fis.close();
            fis = new FileInputStream(file);
            bis = new BufferedInputStream(fis, bufferSize);
            this.position = bis.skip(targetPos);

        }

        @Override
        public long getPos() throws IOException {
            return this.position;
        }

        @Override
        public boolean seekToNewSource(long targetPos) throws IOException {
            return false;
        }

        /*
         * Just forward to the fis
         */
        @Override
        public int available() throws IOException {
            return bis.available();
        }

        @Override
        public void close() throws IOException {
            bis.close();
            fis.close();
        }

        @Override
        public boolean markSupported() {
            return false;
        }

        @Override
        public int read() throws IOException {
            try {
                int value = bis.read();
                if (value >= 0) {
                    this.position++;
                    statistics.incrementBytesRead(1);
                }
                return value;
            } catch (IOException e) {                 // unexpected exception
                throw e;                   // assume native fs error
            }
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {

            try {
                int value = bis.read(b, off, len);
                if (value > 0) {
                    this.position += value;
                    statistics.incrementBytesRead(value);
                }
                return value;
            } catch (IOException e) {                 // unexpected exception
                throw e;                   // assume native fs error
            }
        }

        @Override
        public int read(long position, byte[] b, int off, int len)
                throws IOException {

            try {
                this.seek(position);
                int value = bis.read(b, off, len);
                if (value > 0) {
                    statistics.incrementBytesRead(value);
                }
                return value;
            } catch (IOException e) {
                throw e;
            }
        }

        @Override
        public long skip(long n) throws IOException {
            long value = bis.skip(n);
            if (value > 0) {
                this.position += value;
            }
            return value;
        }

        @Override
        public FileDescriptor getFileDescriptor() throws IOException {
            return fis.getFD();
        }
    }


    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        statistics.incrementReadOps(1);
        if (!exists(f)) {
            throw new FileNotFoundException(f.toString());
        }
        File distributedFile = pathToFile(f);
        String actualFilePath = client.getActualRootPath() + distributedFile.getAbsolutePath();
        File file = new File(actualFilePath);
        if (file.exists()) {
            //logger.info("File exists "+actualFilePath);
            return new FSDataInputStream(
                    new HHFileSystem.LocalFSFileInputStream(file, bufferSize));
        }
        String preDownloadPath = client.checkAndGetPreDownloadedPath(actualFilePath);
        if (preDownloadPath != null) {
            file = new File(preDownloadPath);
            if (file.exists()) {
                // logger.info("File pre-downloaded "+preDownloadPath);
                return new FSDataInputStream(
                        new HHFileSystem.LocalFSFileInputStream(file, bufferSize));
            }
        }
        String parentPath = f.getParent().getParent().toString();
        String distributedPath= getDistributedPath(distributedFile);
        String nodeIdKey = f.getParent().toString().replaceFirst(parentPath, "").substring(1);
        String downloadPath = client.downloadFile(actualFilePath, distributedPath, nodeIdKey);
        file = new File(downloadPath);
        return new FSDataInputStream(
                new HHFileSystem.LocalFSFileInputStream(file, bufferSize));
    }


    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        return null;
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        return null;
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        return false;
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        return false;
    }

    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
        statistics.incrementReadOps(1);
        statistics.incrementLargeReadOps(1);
        File distributeFile = pathToFile(f);
        return new FileStatus[]{client.getFileStatus(f, getDistributedPath(distributeFile))};
    }

    private String getDistributedPath(File distributeFile) {
        return distributeFile.getParentFile().getParentFile().getParent();
    }

    @Override
    public void setWorkingDirectory(Path new_dir) {

    }

    @Override
    public Path getWorkingDirectory() {
        return workingDir;
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        return false;
    }

    /**
     * Checks that the passed URI belongs to this filesystem and returns
     * just the path component. Expects a URI with an absolute path.
     *
     * @param file URI with absolute path
     * @return path component of {file}
     * @throws IllegalArgumentException if URI does not belong to this DFS
     */
    private String getPathName(Path file) {
        checkPath(file);
        String result = file.toUri().getPath();
        if (!DFSUtil.isValidName(result)) {
            throw new IllegalArgumentException("Pathname " + result + " from " +
                    file + " is not a valid DFS filename.");
        }
        return result;
    }


    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        statistics.incrementReadOps(1);
        File distributeFile = pathToFile(f);
        return client.getFileStatus(f, getDistributedPath(distributeFile));
    }


    @Deprecated
    static class DeprecatedRawLocalFileStatus extends FileStatus {
        /* We can add extra fields here. It breaks at least CopyFiles.FilePair().
         * We recognize if the information is already loaded by check if
         * onwer.equals("").
         */
        private boolean isPermissionLoaded() {
            return !super.getOwner().isEmpty();
        }

        DeprecatedRawLocalFileStatus(File f, long defaultBlockSize, FileSystem fs) {
            super(f.length(), f.isDirectory(), 1, defaultBlockSize,
                    f.lastModified(), new Path(f.getPath()).makeQualified(fs.getUri(),
                            fs.getWorkingDirectory()));
        }

        @Override
        public FsPermission getPermission() {
            if (!isPermissionLoaded()) {
                loadPermissionInfo();
            }
            return super.getPermission();
        }

        @Override
        public String getOwner() {
            if (!isPermissionLoaded()) {
                loadPermissionInfo();
            }
            return super.getOwner();
        }

        @Override
        public String getGroup() {
            if (!isPermissionLoaded()) {
                loadPermissionInfo();
            }
            return super.getGroup();
        }

        /// loads permissions, owner, and group from `ls -ld`
        private void loadPermissionInfo() {
            IOException e = null;
            try {
                String output = execCommand(new File(getPath().toUri()),
                        Shell.getGetPermissionCommand());
                StringTokenizer t =
                        new StringTokenizer(output, Shell.TOKEN_SEPARATOR_REGEX);
                //expected format
                //-rw-------    1 username groupname ...
                String permission = t.nextToken();
                if (permission.length() > FsPermission.MAX_PERMISSION_LENGTH) {
                    //files with ACLs might have a '+'
                    permission = permission.substring(0,
                            FsPermission.MAX_PERMISSION_LENGTH);
                }
                setPermission(FsPermission.valueOf(permission));
                t.nextToken();

                String owner = t.nextToken();
                // If on windows domain, token format is DOMAIN\\user and we want to
                // extract only the user name
                if (Shell.WINDOWS) {
                    int i = owner.indexOf('\\');
                    if (i != -1)
                        owner = owner.substring(i + 1);
                }
                setOwner(owner);

                setGroup(t.nextToken());
            } catch (Shell.ExitCodeException ioe) {
                if (ioe.getExitCode() != 1) {
                    e = ioe;
                } else {
                    setPermission(null);
                    setOwner(null);
                    setGroup(null);
                }
            } catch (IOException ioe) {
                e = ioe;
            } finally {
                if (e != null) {
                    throw new RuntimeException("Error while running command to get " +
                            "file permissions : " +
                            StringUtils.stringifyException(e));
                }
            }
        }

        static String execCommand(File f, String... cmd) throws IOException {
            String[] args = new String[cmd.length + 1];
            System.arraycopy(cmd, 0, args, 0, cmd.length);
            args[cmd.length] = f.getCanonicalPath();
            String output = Shell.execCommand(args);
            return output;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            if (!isPermissionLoaded()) {
                loadPermissionInfo();
            }
            super.write(out);
        }
    }

    @Override
    public void close() throws IOException {
        client.close();
        super.close();
    }
}
