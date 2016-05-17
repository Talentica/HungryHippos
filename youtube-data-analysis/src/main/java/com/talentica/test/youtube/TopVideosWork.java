package com.talentica.test.youtube;
import java.io.Serializable;
import java.util.TreeMap;

import com.talentica.hungryHippos.client.domain.ExecutionContext;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.client.domain.Work;

public class TopVideosWork implements Work, Serializable {

	private static final long serialVersionUID = -5931349264723731947L;

	protected int[] dimensions;

	protected int primaryDimension;

	private TreeMap<Double, YoutubeVideo> topRatedVideos = new TreeMap<>();

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
			if (topRatedVideos.size() >= 10) {
				Double largestVideo = topRatedVideos.lastKey();
				topRatedVideos.remove(largestVideo);
			}
			addNewHighlyRatedVideo(executionContext, rating);
		}
	}

	private void addNewHighlyRatedVideo(ExecutionContext executionContext, double rating) {
		String videoId = ((MutableCharArrayString) executionContext.getValue(0)).toString();
		String uploader = ((MutableCharArrayString) executionContext.getValue(1)).toString();
		String category = ((MutableCharArrayString) executionContext.getValue(3)).toString();
		topRatedVideos.put(rating, new YoutubeVideo(videoId, uploader, category, rating));
		topRating = rating;
	}

	@Override
	public void calculate(ExecutionContext executionContext) {
		executionContext.saveValue(6, topRatedVideos);
	}

	@Override
	public void reset() {
		topRatedVideos.clear();
	}

}
