package PCSG.util;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class StemAnalyzer extends StopwordAnalyzerBase {
	
	private static final String stopwordsPath = "stopwords.txt";

	@Override
	protected TokenStreamComponents createComponents(String arg0) {
		Tokenizer tokenizer = new StandardTokenizer();
        TokenFilter lowerCaseFilter = new LowerCaseFilter(tokenizer);
        TokenFilter stopFilter = new StopFilter(lowerCaseFilter, buildCharArraySetFromArray(stopwordsPath)); 
        TokenFilter stemFilter = new PorterStemFilter(stopFilter);
        return new TokenStreamComponents(tokenizer, stemFilter);  
    }
	
	private CharArraySet buildCharArraySetFromArray(String filepath) {
		List<String> stopwords = new ArrayList<>();
		InputStreamReader reader = new InputStreamReader(StemAnalyzer.class.getResourceAsStream(filepath));
        try (BufferedReader bufferedReader = new BufferedReader(reader)) {
			String line = null;
			while((line = bufferedReader.readLine()) != null) {
				stopwords.add(line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
        CharArraySet set = new CharArraySet(stopwords, true);  
        return set;  
    }
}
