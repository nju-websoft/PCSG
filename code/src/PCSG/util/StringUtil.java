package PCSG.util;

import org.tartarus.snowball.ext.PorterStemmer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtil {

	public static String getKeywordsStem(String keywords) {
		String[] words = keywords.split("\\s+");
		StringBuilder sb = new StringBuilder();
		PorterStemmer stemmer = new PorterStemmer();
		for(String word : words) {
			stemmer.setCurrent(word);
			stemmer.stem();
			sb.append(stemmer.getCurrent()+" ");
		}
		return sb.substring(0, sb.length()-1);
	}

	public static String processCamelCase(String label){
		Pattern pattern = Pattern.compile("[a-z]+|[A-Z]+[a-z]*");
		Matcher matcher = pattern.matcher(label);
		List<String> list = new ArrayList<>();
		while(matcher.find())
			list.add(matcher.group());
		StringBuilder sb = new StringBuilder();
		for(int i=0; i<list.size(); i++)
			sb.append(list.get(i)+" ");
		return sb.substring(0, sb.length()).trim();
	}

	public static String processKeywordNew(String keyword) {
		keyword = keyword.replaceAll("\"|#", "");
		keyword = keyword.replaceAll("_|-|\\\\|/|\\(|\\)|\\.\\.\\.|:|,|;|\\?|!|\\+|~|&|\\$|%|\\^|@|\\*|=|<|>|\\[|\\]|\\{|\\}|\\|", " ");
		keyword = keyword.replaceAll("u00..", " ").trim();
		String[] labelSplit = keyword.split("\\s+");
		if (labelSplit.length == 0)
			return "";
		Set<String> set = new HashSet<>();
		List<String> list = new ArrayList<>();
		for (String ls : labelSplit) {
			if(Pattern.matches("[a-z]*[[A-Z]+[a-z]*]+", ls)) { // AaBbCc or aaBbCc
				Pattern pattern = Pattern.compile("[a-z]+|[A-Z]+[a-z]*");
				Matcher matcher = pattern.matcher(ls);
				while(matcher.find())
					list.add(matcher.group());
			} else
				list.add(ls);
		}
		StringBuilder sb = new StringBuilder();
		for(String ls : list) {
			PorterStemmer stemmer = new PorterStemmer();
			stemmer.setCurrent(ls.toLowerCase());
			stemmer.stem();
			String currentStr = stemmer.getCurrent();
			if (!set.contains(currentStr)) { /** Delete duplicate */
				sb.append(currentStr).append(" ");
				set.add(currentStr);
			}
		}
		keyword = sb.toString().trim();
		return keyword;
	}

	public static String processLabel(String label){
		// replace what lucene cannot process
		String keyword = label.replaceAll("\"|#", "");
		keyword = keyword.replaceAll("_|-|\\\\|/|\\(|\\)|\\.\\.\\.|:|,|;|\\?|!|\\+|~|&|\\$|%|\\^|@|\\*|=|<|>|\\[|\\]|\\{|\\}|\\|", " ");
		keyword = keyword.replaceAll("u00..", " ").trim();
//		System.out.println(keyword);
		String[] labelSplit = keyword.split("\\s+");
		if(labelSplit.length == 0)
			return "";
		StringBuilder sb = new StringBuilder();
		List<String> list = new ArrayList<>();
		for (String ls : labelSplit) {
			if(Pattern.matches("[a-z]*[[A-Z]+[a-z]*]+", ls)) { // AaBbCc or aaBbCc
				Pattern pattern = Pattern.compile("[a-z]+|[A-Z]+[a-z]*");
				Matcher matcher = pattern.matcher(ls);
				while(matcher.find())
					list.add(matcher.group());
			} else
				list.add(ls);
		}
		for(String ls : list) {
			PorterStemmer stemmer = new PorterStemmer();
			stemmer.setCurrent(ls.toLowerCase());
			stemmer.stem();
			sb.append(stemmer.getCurrent()+" ");
		}
		keyword = sb.toString().trim();
		return keyword;
	}
}
