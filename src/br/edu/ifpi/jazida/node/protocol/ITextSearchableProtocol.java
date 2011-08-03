package br.edu.ifpi.jazida.node.protocol;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.ipc.VersionedProtocol;

import br.edu.ifpi.jazida.writable.CollectorWritable;
import br.edu.ifpi.jazida.writable.DocumentWritable;
import br.edu.ifpi.jazida.writable.ExplanationWritable;
import br.edu.ifpi.jazida.writable.FieldSelectorWritable;
import br.edu.ifpi.jazida.writable.FilterWritable;
import br.edu.ifpi.jazida.writable.QueryWritable;
import br.edu.ifpi.jazida.writable.SortWritable;
import br.edu.ifpi.jazida.writable.TermWritable;
import br.edu.ifpi.jazida.writable.TopDocsWritable;
import br.edu.ifpi.jazida.writable.TopFieldDocsWritable;
import br.edu.ifpi.jazida.writable.WeightWritable;

public interface ITextSearchableProtocol extends VersionedProtocol {
	
	public static final long versionID = 0;
	
	public void close();

	public DocumentWritable doc(IntWritable arg0);

	public DocumentWritable doc(IntWritable arg0, FieldSelectorWritable arg1);

	public IntWritable docFreq(TermWritable arg0);

	public IntWritable[] docFreqs(TermWritable[] arg0);

	public ExplanationWritable explain(WeightWritable arg0, IntWritable arg1);

	public IntWritable maxDoc();

	public QueryWritable rewrite(QueryWritable arg0);

	public void search(WeightWritable arg0, FilterWritable arg1, CollectorWritable arg2);

	public TopDocsWritable search(WeightWritable arg0, FilterWritable arg1, IntWritable arg2);

	public TopFieldDocsWritable search(WeightWritable arg0, FilterWritable arg1, IntWritable arg2, SortWritable arg3);
	
}
