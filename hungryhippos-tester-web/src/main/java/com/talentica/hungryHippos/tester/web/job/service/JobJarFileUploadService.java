package com.talentica.hungryHippos.tester.web.job.service;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.UUID;

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
@RequestMapping("/job")
public class JobJarFileUploadService {

	@Value("${jobmatrix.jars.dir}")
	private String ROOT;

	@RequestMapping(method = RequestMethod.POST, value = "jar/upload")
	public @ResponseBody JobJarFileUploadServiceResponse uploadJobJarFile(@RequestParam("file") MultipartFile file) {
		JobJarFileUploadServiceResponse fileUploadServiceResponse = new JobJarFileUploadServiceResponse();
		if (file.isEmpty()) {
			ServiceError serviceError = new ServiceError("File is empty. Please provide valid job jar file.",
					"File upload failed: " + file.getName() + " because the file was empty");
			fileUploadServiceResponse.setError(serviceError);
			return fileUploadServiceResponse;
		}
		try {
			String jobUuid = UUID.randomUUID().toString().toUpperCase();
			String directoryPath = ROOT + File.separator + jobUuid + File.separator;
			new File(directoryPath).mkdirs();
			File jobJarFile = new File(directoryPath + file.getOriginalFilename());
			BufferedOutputStream stream = new BufferedOutputStream(new FileOutputStream(jobJarFile));
			FileCopyUtils.copy(file.getInputStream(), stream);
			stream.close();
			fileUploadServiceResponse.setUploadedFileSize(jobJarFile.length());
			fileUploadServiceResponse.setJobUuid(jobUuid);
		} catch (Exception e) {
			ServiceError serviceError = new ServiceError("There was an error while uploading job jar file.",
					"File upload failed: " + file.getName() + " because " + e.getMessage());
			fileUploadServiceResponse.setError(serviceError);
			return fileUploadServiceResponse;
		}
		return fileUploadServiceResponse;
	}

}