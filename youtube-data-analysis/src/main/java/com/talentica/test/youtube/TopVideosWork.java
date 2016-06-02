package com.talentica.test.youtube;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import com.talentica.hungryHippos.client.domain.ExecutionContext;
import com.talentica.hungryHippos.client.domain.InvalidRowExeption;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.client.domain.Work;

public class TopVideosWork implements Work, Serializable {

	private static final long serialVersionUID = -5931349264723731947L;

	protected int[] dimensions;

	protected int primaryDimension;

	private TreeMap<Double, List<YoutubeVideo>> topRatedVideos = new TreeMap<>();

	private double topRating = -1;

	public TopVideosWork(int[] dimensions, int primaryDimension) {
		this.dimensions = dimensions;
		this.primaryDimension = primaryDimension;
	}

	@Override
	public void processRow(ExecutionContext executionContext) {
		double rating = ((Double) executionContext.getValue(6));
		if (topRating == -1) {
			addNewHighlyRatedVideo(executionContext, rating);
		} else if (rating > topRating) {
			if (getAllVideos().size() >= 10) {
				List<YoutubeVideo> smallestVideos = topRatedVideos.firstEntry().getValue();
				smallestVideos.remove(0);
			}
			addNewHighlyRatedVideo(executionContext, rating);
		}
	}

	private List<YoutubeVideo> getAllVideos() {
		List<YoutubeVideo> allvideos = new ArrayList<>();
		topRatedVideos.forEach((rating, videos) -> allvideos.addAll(videos));
		return allvideos;
	}

	private void addNewHighlyRatedVideo(ExecutionContext executionContext, double rating){
		String videoId = ((MutableCharArrayString) executionContext.getValue(0)).toString();
		String uploader = ((MutableCharArrayString) executionContext.getValue(1)).toString();
		String category = ((MutableCharArrayString) executionContext.getValue(3)).toString();
		List<YoutubeVideo> videos = topRatedVideos.get(rating);
		if (videos == null) {
			videos = new ArrayList<YoutubeVideo>();
			topRatedVideos.put(rating, videos);
		}
		videos.add(new YoutubeVideo(videoId, uploader, category, rating));
		topRating = rating;
	}

	@Override
	public void calculate(ExecutionContext executionContext) {
		executionContext.saveValue(6, getAllVideos());
	}

	@Override
	public void reset() {
		topRatedVideos.clear();
	}

}
