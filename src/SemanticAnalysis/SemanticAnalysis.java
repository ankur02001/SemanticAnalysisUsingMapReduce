/////////////////////////////////////////////////////////////////////
//  SemanticAnalysis.java - Used Map reduce to Analyse semantic    //
//                                                                 //
//  ver 1.0                                                        //
//  Language:      Eclipse , Java                                  //
//  Platform:      Dell, Windows 8.1                               //
//  Application:   Semantic Analysis using Map Reduce              //
//  Author:		   Ankur Pandey , Nisha Choudhary                  //
/////////////////////////////////////////////////////////////////////
package SemanticAnalysis;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;
import java.util.*;
import java.util.Map.Entry;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.Iterator;

/////////////////////////////////////////////////////////////////////////////////////////////////
//////  Semantic Analysis 
/////////////////////////////////////////////////////////////////////////////////////////////////

public class SemanticAnalysis {
	public static Tree tree;
	
/////////////////////////////////////////////////////////////////////////////////////////////////
////// XMLParsingClass
/////////////////////////////////////////////////////////////////////////////////////////////////
	public static class XMLParsing extends TextInputFormat {

		public static final String START_TAG_KEY = "XML.start";
		public static final String END_TAG_KEY = "XML.end";

		public RecordReader<LongWritable, Text> createRecordReader(
				InputSplit split, TaskAttemptContext context) {
			return new XmlRecordReader();
		}

		public static class XmlRecordReader extends
		RecordReader<LongWritable, Text> {
			private byte[] startTag;
			private byte[] endTag;
			private long start;
			private long end;
			private FSDataInputStream fsin;
			private DataOutputBuffer buffer = new DataOutputBuffer();

			private LongWritable key = new LongWritable();
			private Text value = new Text();

			@Override
			public void initialize(InputSplit split, TaskAttemptContext context)
					throws IOException, InterruptedException {
				Configuration conf = context.getConfiguration();
				startTag = conf.get(START_TAG_KEY).getBytes("utf-8");
				endTag = conf.get(END_TAG_KEY).getBytes("utf-8");
				FileSplit fileSplit = (FileSplit) split;

				// open the file and seek to the start of the split
				start = fileSplit.getStart();
				end = start + fileSplit.getLength();
				Path file = fileSplit.getPath();
				FileSystem fs = file.getFileSystem(conf);
				fsin = fs.open(fileSplit.getPath());
				fsin.seek(start);

			}

			@Override
			public boolean nextKeyValue() throws IOException,
			InterruptedException {
				if (fsin.getPos() < end) {
					if (readUntilMatch(startTag, false)) {
						try {
							buffer.write(startTag);
							if (readUntilMatch(endTag, true)) {
								key.set(fsin.getPos());
								value.set(buffer.getData(), 0,
										buffer.getLength());
								return true;
							}
						} finally {
							buffer.reset();
						}
					}
				}
				return false;
			}

			@Override
			public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
				return key;
			}

			@Override
			public Text getCurrentValue() throws IOException,
			InterruptedException {
				return value;
			}

			@Override
			public void close() throws IOException {
				fsin.close();
			}

			@Override
			public float getProgress() throws IOException {
				return (fsin.getPos() - start) / (float) (end - start);
			}

			private boolean readUntilMatch(byte[] match, boolean withinBlock)
					throws IOException {
				int i = 0;
				while (true) {
					int b = fsin.read();
					// end of file:
					if (b == -1)
						return false;
					// save to buffer:
					if (withinBlock)
						buffer.write(b);
					// check if we're matching:
					if (b == match[i]) {
						i++;
						if (i >= match.length)
							return true;
					} else
						i = 0;
					// see if we've passed the stop point:
					if (!withinBlock && i == 0 && fsin.getPos() >= end)
						return false;
				}
			}
		}
	}
/////////////////////////////////////////////////////////////////////////////////////////////////
//////     Mapper Class
/////////////////////////////////////////////////////////////////////////////////////////////////
	public static class SymenticMapper extends Mapper<LongWritable, Text, Text, Text> {
		public static Map<String, ArrayList<String>> originalMap =
				new HashMap<String, ArrayList<String>>();

		public static String getValue(String Value){
			String TREETOKEN= "@";
			StringTokenizer TreeToken = new StringTokenizer(Value, TREETOKEN);
			TreeToken.nextToken();
			return TreeToken.nextToken();
		}
		public static void writingToReducer(String key,String value,Mapper.Context context) throws IOException,
		InterruptedException {
			if(!originalMap.containsKey(key)){
				ArrayList<String> temp = new ArrayList<String>(); 
				temp.add(value);
				originalMap.put(key,temp);
			}else{
				ArrayList<String> temp = originalMap.get(key); 
				temp.add(value);
				originalMap.put(key, temp);
			}
			for(Map.Entry<String,ArrayList<String>> iter1: originalMap.entrySet()){
				String ID1 = iter1.getKey();
				ArrayList<String> childID1s = iter1.getValue();
				for(String childID1:childID1s){
					for(Map.Entry<String,ArrayList<String>> iter2: originalMap.entrySet()){
						String ID2 = iter2.getKey();
						ArrayList<String> childID2s = iter2.getValue();
						for(String childID2:childID2s){
							System.out.println("");
							String keyParm_ = ID1;
							String valParm_ = childID2+"@"+getValue(childID1);
							System.out.println("Key=== " + keyParm_ );
							System.out.println(" Value=== " + valParm_ );
							context.write(new Text(keyParm_), new Text(valParm_));
						}
					}
					//}
				}
			}
		}

//----------<	@Override map >-----------------------------------------------------
		@Override
		protected void map(LongWritable key, Text value, Mapper.Context context)
				throws IOException, InterruptedException {
			String document = value.toString();
			System.out.println("‘" + document + "‘");
			try {
				XMLStreamReader reader = XMLInputFactory.newInstance()
						.createXMLStreamReader(
								new ByteArrayInputStream(document.getBytes()));
				String propertyID = "";
				String propertylink = "";
				String currentElement = "";
				String ChildNode ="";
				while (reader.hasNext()) {
					propertylink = "";
					int code = reader.next();
					switch (code) {
					case XMLStreamConstants.START_ELEMENT: 
						currentElement = reader.getLocalName();
						break;
					case XMLStreamConstants.CHARACTERS:
						if (currentElement.equalsIgnoreCase("UserID")) {
							propertyID += reader.getText();
							//	System.out.println("propertName" + propertyID);
						} else if (currentElement.equalsIgnoreCase("link")) {
							propertylink += reader.getText();

							String TREETOKEN= "/";
							StringTokenizer TreeToken = new StringTokenizer(propertylink, TREETOKEN);
							String parent="";
							if(TreeToken.hasMoreTokens())
								parent = TreeToken.nextToken();
							//	System.out.println(" child= " + parent);
							if(parent!=""){
								tree.addNode(parent,"SemanticRoot");
								while (TreeToken.hasMoreTokens()) {
									String child = TreeToken.nextToken();
									//		System.out.println(" child= " + child);
									tree.addNode(child,parent);
									parent = child;
									ChildNode = child;
								}	
							}
						}
						break;
					}
				}
				reader.close();

				//System.out.println(" writingToReducer == " + propertyID.trim()+"@"+ChildNode);
				writingToReducer(propertyID.trim(),propertyID.trim()+"@"+ChildNode,context);

			} catch (Exception e) {
				throw new IOException(e);

			}

		}
	}


/////////////////////////////////////////////////////////////////////////////////////////////////
////// Reducer Class 
/////////////////////////////////////////////////////////////////////////////////////////////////
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void setup(Context context) throws IOException,
		InterruptedException {
			//	context.write(new Text("<configuration>"), null);
		}

		public static String getID(String Value){
			String TREETOKEN= "@";
			StringTokenizer TreeToken = new StringTokenizer(Value, TREETOKEN);
			return TreeToken.nextToken();
		}
		
//----------<	Semantic Score Search >-----------------------------------------------------
		public Integer semanticScoreSearch(String nodeSearch1,String nodeSearch2){
			Iterator<Node> depthIterator = tree.iterator("SemanticRoot");
			Node nodeSearchforID1 = null;
			while(depthIterator.hasNext()){
				nodeSearchforID1 = depthIterator.next();
				if(nodeSearchforID1.getIdentifier().contains(nodeSearch1)){
					break;
				}
				System.out.println(" " + nodeSearchforID1.getParent());
			}
			Iterator<Node> depthIterator2 = tree.iterator("SemanticRoot");

			Node nodeSearchforID2 = null;
			while(depthIterator2.hasNext()){
				nodeSearchforID2 = depthIterator2.next();
				if(nodeSearchforID2.getIdentifier().contains(nodeSearch2)){
					break;
				}
				System.out.println(" " + nodeSearchforID1.getParent());
			}

			return scoreCalculate(nodeSearchforID1,nodeSearchforID2);
		}
		
//----------<	Score Calculate >-----------------------------------------------------
		public Integer scoreCalculate(Node nodeSearchforID1, Node nodeSearchforID2){
			Map<String,Integer> mapID1 =new HashMap<String,Integer>();
			Map<String,Integer> mapID2 =new HashMap<String,Integer>();
			while(( nodeSearchforID1.getIdentifier() != "SemanticRoot" 
					&& nodeSearchforID2.getIdentifier() != "SemanticRoot" )|| 
					nodeSearchforID1.getIdentifier() != nodeSearchforID2.getIdentifier()){
				mapID1.put(nodeSearchforID1.getIdentifier(),nodeSearchforID1.getLevel());
				mapID2.put(nodeSearchforID2.getIdentifier(),nodeSearchforID2.getLevel());
				//case1 
				if(nodeSearchforID1.getIdentifier() == nodeSearchforID2.getIdentifier()){
					return nodeSearchforID2.getLevel();
				}
				//case2 
				if(mapID1.containsKey(nodeSearchforID2.getIdentifier())){
					return mapID1.get(nodeSearchforID2.getIdentifier());
				}
				//case3 
				if(mapID2.containsKey(nodeSearchforID1.getIdentifier())){
					return mapID2.get(nodeSearchforID1.getIdentifier());
				}
				nodeSearchforID1 = nodeSearchforID1.getParent();
				nodeSearchforID2 = nodeSearchforID2.getParent();
			}
			if(( nodeSearchforID1.getIdentifier()!= "SemanticRoot")&&
					( nodeSearchforID2.getIdentifier()!="SemanticRoot")
				     && (nodeSearchforID2.getIdentifier() == nodeSearchforID2.getIdentifier())){
				return nodeSearchforID2.getLevel();
			}
			return 1;
		}

		@Override
		protected void cleanup(Context context) throws IOException,
		InterruptedException {
			//	context.write(new Text("</configuration>"), null);
		}
		
		private Text outputKey = new Text();
//----------<	@Override reducer >-----------------------------------------------------
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			  Set<String> set_ = new HashSet<String>();
			  Set<String> ID2set_ = new HashSet<String>();
			  Set<String> ID1set_ = new HashSet<String>();
			  Map<String,String> scoreMappp =new HashMap<String,String>();
            // Removing dublicates
			for (Text value : values) {
				set_.add(value.toString());
			}
            // Parsing for different input
			for( String str : set_){
				String splitter[] = str.split("@");
				ID1set_.add(splitter[2]);	
				ID2set_.add(splitter[0] + "@"+splitter[1]);
			}
           // Calculating Score
			for(String child1 : ID1set_){
				for(String child2Str: ID2set_){
					String splitter[] = child2Str.split("@");
					String ID2 = splitter[0];
					String child2 = splitter[1];
					Integer score = semanticScoreSearch(child1,child2);
					if(!scoreMappp.containsKey(ID2)){
						scoreMappp.put(ID2,Integer.toString(score));
					}else{
						Integer tempScore = new Integer(scoreMappp.get(ID2)); 
						score += tempScore;
						scoreMappp.put(ID2, Integer.toString(score));
					}
				}
			} 
		  // Writing to output 
			for(Entry<String, String> iterWriting: scoreMappp.entrySet()){
				String ID2 = iterWriting.getKey();
				String score = iterWriting.getValue();
				System.out.println("KeyID1=== " + key );
				System.out.println("KeyID2=== " + ID2 );
				System.out.println("Score=== " + score );
				String temp = ID2 + "::" + score;
				context.write(new Text(key), new Text(temp));
			}
		}
	}
	
   //----------<	main >-----------------------------------------------------
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		// Root Node creation
		SemanticAnalysis.tree = new Tree(); 
		SemanticAnalysis.tree.addNode("SemanticRoot");
		
		// Xml parsing
		conf.set("XML.start", "<property>");
		conf.set("XML.end", "</property>");
		
		// Setting Job 
		Job job = new Job(conf);
		job.setJarByClass(SemanticAnalysis.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(SemanticAnalysis.SymenticMapper.class);
		job.setReducerClass(SemanticAnalysis.Reduce.class);
		job.setInputFormatClass(XMLParsing.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		
		// Displaying Tree 
		SemanticAnalysis.tree.display("SemanticRoot");
	}
}