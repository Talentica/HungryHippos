package com.talentica.test.youtube;

import java.io.Serializable;

public class YoutubeVideo implements Serializable {

	private static final long serialVersionUID = -7709911700952447513L;

	private String videoId;

	private String uploader;

	private String category;

	private double rating;

	public YoutubeVideo(String videoId, String uploader, String category, double rating) {
		setVideoId(videoId);
		setUploader(uploader);
		setCategory(category);
		setRating(rating);
	}

	public String getVideoId() {
		return videoId;
	}

	public void setVideoId(String videoId) {
		this.videoId = videoId;
	}

	public String getUploader() {
		return uploader;
	}

	public void setUploader(String uploader) {
		this.uploader = uploader;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public double getRating() {
		return rating;
	}

	public void setRating(double rating) {
		this.rating = rating;
	}

	@Override
	public String toString() {
		return "Video Id: " + videoId + ",Uploader:" + uploader + ",Category: " + category + ",Rating: " + rating + " ";
	}

	@Override
	public int hashCode() {
		return videoId.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (obj != null && obj instanceof YoutubeVideo) {
			YoutubeVideo otherVideo = (YoutubeVideo) obj;
			if (otherVideo.getVideoId() != null && videoId != null) {
				return otherVideo.getVideoId().equals(videoId);
			}
		}
		return false;
	}

}
