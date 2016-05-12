package com.github.sharplog.samples.spark.ldasample;

import java.util.List;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.Segment;
import com.hankcs.hanlp.seg.Dijkstra.DijkstraSegment;
import com.hankcs.hanlp.seg.NShort.NShortSegment;
import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.tokenizer.IndexTokenizer;
import com.hankcs.hanlp.tokenizer.NLPTokenizer;
import com.hankcs.hanlp.tokenizer.SpeedTokenizer;
import com.hankcs.hanlp.tokenizer.StandardTokenizer;

public class HanLPExample {
	public static void main(String[] args) {
		System.out.println(HanLP.segment("你好，欢迎使用HanLP汉语处理包！"));
		
		// 标准分词，分词结果包含词性
		List<Term> termList = StandardTokenizer.segment("商品和服务");
		System.out.println(termList);
		for( int i = 0; i < termList.size(); i++){
			System.out.println(termList.get(i).word + "/" + termList.get(i).nature);
		}
		termList = StandardTokenizer.segment("中国科学院计算技术研究所的宗成庆教授正在教授自然语言处理课程");
		System.out.println(termList);
		
		// NLP分词，会执行全部命名实体识别和词性标注
		termList = NLPTokenizer.segment("中国科学院计算技术研究所的宗成庆教授正在教授自然语言处理课程");
		System.out.println(termList);
		
		// 索引分词，面向搜索引擎的分词器，能够对长词全切分，另外通过term.offset可以获取单词在文本中的偏移量。
		termList = IndexTokenizer.segment("主副食品");
		for (Term term : termList){
		    System.out.println(term + " [" + term.offset + ":" + (term.offset + term.word.length()) + "]");
		}
		
		// N-最短路径分词，比最短路分词器慢，但是效果稍微好一些，对命名实体识别能力更强。
		// 一般场景下最短路分词的精度已经足够，而且速度比N最短路分词器快几倍，请酌情选择。
		Segment nShortSegment = new NShortSegment().enableCustomDictionary(false).enablePlaceRecognize(true).enableOrganizationRecognize(true);
		Segment shortestSegment = new DijkstraSegment().enableCustomDictionary(false).enablePlaceRecognize(true).enableOrganizationRecognize(true);
		String[] testCase = new String[]{
		        "今天，刘志军案的关键人物,山西女商人丁书苗在市二中院出庭受审。",
		        "刘喜杰石国祥会见吴亚琴先进事迹报告团成员",
		        };
		for (String sentence : testCase){
		    System.out.println("N-最短分词：" + nShortSegment.seg(sentence) + "\n最短路分词：" + shortestSegment.seg(sentence));
		}
		
		//CRF分词，对新词有很好的识别能力，但是开销较大。
		/*
		Segment segment = new CRFSegment();
		segment.enablePartOfSpeechTagging(true);
		termList = segment.seg("你看过穆赫兰道吗");
		System.out.println(termList);
		for (Term term : termList){
		    if (term.nature == null){
		        System.out.println("识别到新词：" + term.word);
		    }
		}
		*/
		
		// 极速分词，是词典最长分词，速度极其快，精度一般。
		String text = "江西鄱阳湖干枯，中国最大淡水湖变成大草原";
        System.out.println(SpeedTokenizer.segment(text));
        long start = System.currentTimeMillis();
        int pressure = 100000;
        for (int i = 0; i < pressure; ++i)
        {
            SpeedTokenizer.segment(text);
        }
        double costTime = (System.currentTimeMillis() - start) / (double)1000;
        System.out.printf("分词速度：%.2f字每秒\n", text.length() * pressure / costTime);
        
        // 中国人名识别
        testCase = new String[]{
                "签约仪式前，秦光荣、李纪恒、仇和等一同会见了参加签约的企业家。",
                "王国强、高峰、汪洋、张朝阳光着头、韩寒、小四",
                "张浩和胡健康复员回家了",
                "王总和小丽结婚了",
                "编剧邵钧林和稽道青说",
                "这里有关天培的有关事迹",
                "龚学平等领导,邓颖超生前",
                };
        Segment segment = HanLP.newSegment().enableNameRecognize(true);
        for (String sentence : testCase)
        {
            //termList = segment.seg(sentence);
        	termList = HanLP.segment(sentence);
            System.out.println(termList);
        }
	}
}
