import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank 
{

	  public static void main(String[] args) throws Exception {

	    String inputFile = args[0];
	    String outputDir = args[1];

	    iterate(inputFile, outputDir);
	  }
	  /**
	   * Initialise les jobs
	   * @param input Fichier de données
	   * @param output Fichier avec les résultats
	   * @throws Exception
	   */
	  public static void iterate(String input, String output) throws Exception {

	    Configuration conf = new Configuration();
	    
	    Path outputPath = new Path(output);
		//On supprime l'ancien dossier s'il existe
	    outputPath.getFileSystem(conf).delete(outputPath, true);
		//On crée un nouveau dossier
	    outputPath.getFileSystem(conf).mkdirs(outputPath);
		//On rajoute notre input.txt (fichier où il y a les données du graphe)
	    Path inputPath = new Path(outputPath, "input.txt");

	    int numNodes = createInputFile(new Path(input), inputPath);

		//Nombre d'iterration du programme
	    int iter = 1;
		//Tolérance du pageRank (elle est souvent très basse)
	    double desiredConvergence = 0.01;

	    while (true) {

	      Path jobOutputPath = new Path(outputPath, String.valueOf(iter));

	      System.out.println("======================================");
	      System.out.println("=  Iteration:    " + iter);
	      System.out.println("=  Input path:   " + inputPath);
	      System.out.println("=  Output path:  " + jobOutputPath);
	      System.out.println("======================================");

		  //Tant que le pageRank est inférieur à la tolérance on calcule le pageRank
	      if (calcPageRank(inputPath, jobOutputPath, numNodes) < desiredConvergence) {
	    	  System.out.println("Convergence is below " + desiredConvergence + ", we're done");
	    	  break;
	      }
	      inputPath = jobOutputPath;
		  //On augmente le nombre d'itérations
	      iter++;
	    }
	  }
	  /**
	   * Créer le fichier input avec les données d'entrée
	   * @param file
	   * @param targetFile
	   * @return
	   * @throws IOException
	   */
	  public static int createInputFile(Path file, Path targetFile)
	      throws IOException {
	    Configuration conf = new Configuration();
	    
	    FileSystem fs = file.getFileSystem(conf);

	    int numNodes = getNumNodes(file);
	    double initialPageRank = 1.0 / (double) numNodes;

	    OutputStream os = fs.create(targetFile);
	    LineIterator iter = IOUtils.lineIterator(fs.open(file), "UTF8");

	    while (iter.hasNext()) {
	      String line = iter.nextLine();

	      String[] parts = StringUtils.split(line);

	      Node node = new Node()
	          .setPageRank(initialPageRank)
	          .setAdjacentNodeNames(
	              Arrays.copyOfRange(parts, 1, parts.length));
	      IOUtils.write(parts[0] + '\t' + node.toString() + '\n', os);
	    }
	    os.close();
	    return numNodes;
	  }
	  /**
	   * Méthode permettant de calculer et de renvoyer le nombre de noeud en fonction du fichier (chaque ligne = 1 noeud)
	   * @param file le fichier pour lequel il faut calculer le nombre de noeud
	   * @return le nombre de noeud du graphe
	   * @throws IOException
	   */
	  public static int getNumNodes(Path file) throws IOException {
	    Configuration conf = new Configuration();
	    
	    FileSystem fs = file.getFileSystem(conf);

	    return IOUtils.readLines(fs.open(file), "UTF8").size();
	  }
	  /**
	   * Permet de calculer le pageRank du noeud
	   * @param inputPath
	   * @param outputPath
	   * @param numNodes
	   * @return
	   * @throws Exception
	   */
	  public static double calcPageRank(Path inputPath, Path outputPath, int numNodes)
	      throws Exception {
	    Configuration conf = new Configuration();
	    conf.setInt(Reduce.CONF_NUM_NODES_GRAPH, numNodes);
	    Job job = Job.getInstance(conf, "PageRankJob");
	    job.setJarByClass(PageRank.class);
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);

	    job.setInputFormatClass(KeyValueTextInputFormat.class);

	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);

	    FileInputFormat.setInputPaths(job, inputPath);
	    FileOutputFormat.setOutputPath(job, outputPath);

	    if (!job.waitForCompletion(true)) {
	      throw new Exception("Job failed");
	    }
		//On retrouve la somme de la tolérence avec avec les compteurs du job
	    long summedConvergence = job.getCounters().findCounter(
	        Reduce.Counter.CONV_DELTAS).getValue();
		//on cacule la tolérence 	
	    double convergence =
	        ((double) summedConvergence /
	            Reduce.CONVERGENCE_SCALING_FACTOR) /
	            (double) numNodes;

	    System.out.println("======================================");
	    System.out.println("=  Num nodes:           " + numNodes);
	    System.out.println("=  Summed convergence:  " + summedConvergence);
	    System.out.println("=  Convergence:         " + convergence);
	    System.out.println("======================================");

	    return convergence;
	  }

  	public static class Map extends Mapper<Text, Text, Text, Text> 
    {
	
		private Text outKey = new Text();
		private Text outValue  = new Text();
	
	  @Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			//On émet la clé et la valeur du noeud
			context.write(key, value);
			//On crée un noeud temporaire à partir de la valeur
			Node node = Node.fromMR(value.toString());
			//S'il a des voisins
			if(node.getAdjacentNodeNames() != null && node.getAdjacentNodeNames().length > 0) {
				//On calcule le pageRank du noeud avec (PageRank du noeud/nombre de voisins du noeud)
			  double outboundPageRank = node.getPageRank() /(double)node.getAdjacentNodeNames().length;
			  // Pour chaque voisin du noeud on leur propage le PageRank
				  for (int i = 0; i < node.getAdjacentNodeNames().length; i++) {
					//Clé du voisin du noeud
				    String neighbor = node.getAdjacentNodeNames()[i];
				    outKey.set(neighbor);
					//On transmet la valeur du pageRank sortant au voisin du noeud
				    Node adjacentNode = new Node().setPageRank(outboundPageRank);
					//Valeur du voisin du noeud
				    outValue.set(adjacentNode.toString());
					//On émet 
				    context.write(outKey, outValue);
				  }
			}
		}
	  }

  
  public static class Node {
  private double pageRank = 0.25;
  private String[] adjacentNodeNames;
 
 //Champ séparateur. Dans le jeu de données ce sont des tabulations
  public static final char fieldSeparator = '\t';
 
 /**
  * Méthode renvoyant le pageRank du noeud
  * @return	PageRank du noeud
  */
  public double getPageRank() {
    return pageRank;
  }
 
  /**
   * Méthode permettant de placer la valeur sur un noeud
   * @param pageRank la valeur du pagerank sur le noeud
   * @return le noeud sur lequel nous avons mis la valeur du pageRank
   */
  public Node setPageRank(double pageRank) {
    this.pageRank = pageRank;
    return this;
  }
 /**
  * Méthode renvoyant la liste des voisins du noeud
  * @return la liste des voisins du noeud
  */
  public String[] getAdjacentNodeNames() {
    return adjacentNodeNames;
  }
 
  /**
   * Méthode permettant de placer les voisins sur un noeud
   * @param adjacentNodeNames les voisins du noeuds
   * @return le noeud avec ces différents voisins 
   */
  public Node setAdjacentNodeNames(String[] adjacentNodeNames) {
    this.adjacentNodeNames = adjacentNodeNames;
    return this;
  }
 /**
  * Méthode vérifiant si le noeud contient des voisins
  * @return un booléen en fonction des voisins
  */
  public boolean containsAdjacentNodes() {
    return adjacentNodeNames != null;
  }
 
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(pageRank);
 
    if (getAdjacentNodeNames() != null) {
      sb.append(fieldSeparator)
          .append(StringUtils
              .join(getAdjacentNodeNames(), fieldSeparator));
    }
    return sb.toString();
  }
 /**
  * Méthode permettant de créer un noeud avec une clé et ses voisins
  * @param value valeur de la paire (clé,valeur) pour le map reduce
  * @return	un noeud avec une clé et ses valeurs (voisins)
  * @throws IOException
  */
  public static Node fromMR(String value) throws IOException {
	  //On place les valeurs dans un tableau de string (1	3 => {1,3})
    String[] parts = StringUtils.splitPreserveAllTokens(
        value, fieldSeparator);
    if (parts.length < 1) {
      throw new IOException(
          "Expected 1 or more parts but received " + parts.length);
    }
    Node node = new Node()
		//On met la valeur du pageRank au noeud
        .setPageRank(Double.valueOf(parts[0]));
    if (parts.length > 1) {
		//On cherche ses voisins
      node.setAdjacentNodeNames(Arrays.copyOfRange(parts, 1,
          parts.length));
    }
    return node;
  }
  }

  public static class Reduce extends Reducer<Text, Text, Text, Text> {

	public static final double CONVERGENCE_SCALING_FACTOR = 1000.0;
	public static final double DAMPING_FACTOR = 0.85;
	public static String CONF_NUM_NODES_GRAPH = "pagerank.numnodes";
	private int numberOfNodesInGraph;
	
	public static enum Counter {
	CONV_DELTAS
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		numberOfNodesInGraph = context.getConfiguration().getInt(CONF_NUM_NODES_GRAPH, 0);
	}
	
	private Text outValue = new Text();
	
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
	
		//Somme du pageRank pour une clé
		double summedPageRanks = 0;
		//Le noeud original. Ici le noeud aura pour clé "1" (1	3 ; 1  4) 
		Node originalNode = new Node();
		
		for (Text textValue : values) {
			//Pour chaque valeur, on crée un noeud temporaire
		  Node node = Node.fromMR(textValue.toString());
			//S'il contient des voisins alors c'est un noeud parent
		  if (node.containsAdjacentNodes()) {
		    originalNode = node;
		  } else {
			  //Sinon on additionne le pageRank au noeud temporaire
		    summedPageRanks += node.getPageRank();
		  }
		}

		//Le facteur d'amortissement est soustrait par 1 puis le resultat est divisé par le nombre de page dans le graphe
		double dampingFactor = ((1.0 - DAMPING_FACTOR) / (double) numberOfNodesInGraph);
		//Ce resultat est ensuite ajouté à la somme de pagerank du noeud multiplié par le facteur d'amortissement
		double newPageRank = dampingFactor + (DAMPING_FACTOR * summedPageRanks);
		
		double delta = originalNode.getPageRank() - newPageRank;
		
		originalNode.setPageRank(newPageRank);
		
		outValue.set(originalNode.toString());
		
		context.write(key, outValue);
		int scaledDelta = Math.abs((int) (delta * CONVERGENCE_SCALING_FACTOR));
		context.getCounter(Counter.CONV_DELTAS).increment(scaledDelta);
	}
}

	}
