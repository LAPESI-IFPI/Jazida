package br.edu.ifpi.jazida.node.protocol;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;

import br.edu.ifpi.jazida.writable.BufferedImageWritable;
import br.edu.ifpi.jazida.writable.SearchResultWritable;
import br.edu.ifpi.opala.searching.SearcherImage;
import br.edu.ifpi.opala.searching.SearcherImageImpl;

public class ImageSearcherProtocol implements IImageSearchProtocol {

	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		return 0;
	}

	@Override
	public SearchResultWritable search(	BufferedImageWritable image,
										IntWritable limit) {
		try {
			SearcherImage searcher = new SearcherImageImpl();
			return new SearchResultWritable(searcher.search(image.getBufferedImage(), limit.get()));
		} catch (Exception e) {
			return new SearchResultWritable(e);
		}
	}

}
