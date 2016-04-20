package com.talentica.hungryHippos.tester.web.job.service;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Base64;
import java.util.UUID;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.util.FileCopyUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import com.talentica.hungryHippos.tester.web.service.ServiceError;

@Controller
@RequestMapping("/secure/job")
public class JobJarFileUploadService {

	private Logger LOGGER = LoggerFactory.getLogger(JobJarFileUploadService.class);

	@Value("${jobmatrix.jars.dir}")
	private String JOB_MATRIX_JAR_DIRECTORY;

	private static final long HUNDRED_MBS = 1024 * 1024 * 100;

	@RequestMapping(method = RequestMethod.POST, value = "jar/upload")
	public @ResponseBody JobJarFileUploadServiceResponse uploadJobJarFile(
			@RequestParam("jobMatrixClassName") String jobMatrixClassName, @RequestParam("file") MultipartFile file) {
		JobJarFileUploadServiceResponse fileUploadServiceResponse = new JobJarFileUploadServiceResponse();
		try {
			if (file.isEmpty()) {
				ServiceError serviceError = new ServiceError("File is empty. Please provide valid a job jar file.",
						"File upload failed: " + file.getName() + " because the file was empty");
				fileUploadServiceResponse.setError(serviceError);
				return fileUploadServiceResponse;
			}

			if (StringUtils.isBlank(jobMatrixClassName)) {
				ServiceError serviceError = new ServiceError("Please provide with valid job matrix name. It is empty.",
						"Job matrix class name is empty.");
				fileUploadServiceResponse.setError(serviceError);
				return fileUploadServiceResponse;
			}

			if (!file.getContentType().contains("java-archive") || !file.getOriginalFilename().contains(".jar")) {
				ServiceError serviceError = new ServiceError(
						"Please upload a valid jar file. Content type should be application/java-archive and file should have an extension of 'jar'",
						"File upload failed. Invalid file submitted -" + file.getOriginalFilename());
				fileUploadServiceResponse.setError(serviceError);
				return fileUploadServiceResponse;
			}

			if (file.getSize() >= HUNDRED_MBS) {
				ServiceError serviceError = new ServiceError("Maximum file size for uload is 100 MB.",
						"File upload failed: " + file.getOriginalFilename() + ". File size exceeded:" + file.getSize());
				fileUploadServiceResponse.setError(serviceError);
				return fileUploadServiceResponse;
			}

			String jobUuid = UUID.randomUUID().toString().toUpperCase();
			jobUuid = Base64.getUrlEncoder().encodeToString(jobUuid.getBytes());
			String directoryPath = JOB_MATRIX_JAR_DIRECTORY + File.separator + jobUuid;
			new File(directoryPath).mkdirs();
			String uploadedJarFilePath = directoryPath + File.separator + file.getOriginalFilename();
			File jobJarFile = new File(uploadedJarFilePath);
			BufferedOutputStream stream = new BufferedOutputStream(new FileOutputStream(jobJarFile));
			FileCopyUtils.copy(file.getInputStream(), stream);
			stream.close();

			ServiceError error = validateIfClassPresentInUploadedJobJarFile(uploadedJarFilePath, jobMatrixClassName);
			if (error != null) {
				fileUploadServiceResponse.setError(error);
				new File(uploadedJarFilePath).delete();
				new File(directoryPath).delete();
				return fileUploadServiceResponse;
			}
			fileUploadServiceResponse.setUploadedFileSize(jobJarFile.length());
			fileUploadServiceResponse.setJobUuid(jobUuid);
		} catch (Exception e) {
			LOGGER.error("Error occurred while processing job jar upload request.", e);
			ServiceError serviceError = new ServiceError("There was an error while uploading job jar file.",
					"File upload failed: " + file.getName() + " because " + e.getMessage());
			fileUploadServiceResponse.setError(serviceError);
			return fileUploadServiceResponse;
		}
		return fileUploadServiceResponse;
	}

	private ServiceError validateIfClassPresentInUploadedJobJarFile(String jobJarFilePath, String jobMatrixClassName) {
		ServiceError error = null;
		ZipFile zipFile = null;
		try {
			String jobMatrixClassEntryPathInZip = jobMatrixClassName.replaceAll("\\.", File.separator) + ".class";
			zipFile = new ZipFile(jobJarFilePath);
			ZipArchiveEntry entry = zipFile.getEntry(jobMatrixClassEntryPathInZip);
			if (entry == null) {
				error = new ServiceError(
						"Job matrix class: " + jobMatrixClassName + " does not exist in uploaded jar file.",
						"Job class not present.");
			}
		} catch (Exception exception) {
			error = new ServiceError("Please provide with valid job jar file.",
					"Job JAR file uploaded is invalid." + exception.getMessage());
		} finally {
			ZipFile.closeQuietly(zipFile);
		}
		return error;
	}

}