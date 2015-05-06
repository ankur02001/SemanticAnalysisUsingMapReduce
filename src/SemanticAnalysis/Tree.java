/////////////////////////////////////////////////////////////////////
//  Tree.java - Builds in Memory tree for Link data                //
//                                                                 //
//  ver 1.0                                                        //
//  Language:      Eclipse , Java                                  //
//  Platform:      Dell, Windows 8.1                               //
//  Application:   Semantic Analysis using Map Reduce              //
//  Author:		   Ankur Pandey , Nisha Choudhary                  //
/////////////////////////////////////////////////////////////////////

package SemanticAnalysis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class Tree {
	public enum TraversalStrategy {
		 DEPTH_FIRST,
		 BREADTH_FIRST
	}
    private final static int ROOT = 0;

    private HashMap<String, Node> nodes;
    private TraversalStrategy traversalStrategy;

    // Constructors
    public Tree() {
        this(TraversalStrategy.DEPTH_FIRST);
    }

    public Tree(TraversalStrategy traversalStrategy) {
        this.nodes = new HashMap<String, Node>();
        this.traversalStrategy = traversalStrategy;
    }

    // Properties
    public HashMap<String, Node> getNodes() {
        return nodes;
    }

    public TraversalStrategy getTraversalStrategy() {
        return traversalStrategy;
    }

    public void setTraversalStrategy(TraversalStrategy traversalStrategy) {
        this.traversalStrategy = traversalStrategy;
    }

    // Public interface
    public Node addNode(String identifier) {
    	 Node node1 = new Node("STOP",1,null);
    	 Node node = new Node(identifier,1,node1);
         nodes.put(identifier, node);
         
        return node;
    }
    
    public Node searchForPareentAddedChild(String parent){
        Iterator<Node> depthIterator = SemanticAnalysis.tree.iterator("SemanticRoot");
        Node nodeSearchforID1=null;
        while (depthIterator.hasNext()) {
        	nodeSearchforID1 = depthIterator.next();
            if(nodeSearchforID1.getIdentifier().contentEquals(parent)){
            	nodeSearchforID1= nodeSearchforID1.getParent();
            	 return nodeSearchforID1;
            }
            System.out.println(nodeSearchforID1.getParent());
        }
        return nodeSearchforID1;
    }

    public Node addNode(String identifier, String parent) {
    //	searchForPareentAddedChild(parent );
	//	System.out.println(" Testing Ank 1 "
		//		+ "parent= " + parent + " child= " + identifier );

    	Integer  level=1;
    	Integer descendent=0;
    	Node node=null;
    //	System.out.println(" Testing Ank===== ");
    //	 System.out.println(nodes.get(parent).getChildren());
    	 
    	if(!nodes.get(parent).getChildren().contains(identifier)){
    //		System.out.println(" Testing Ank 2"
   // 				+ "parent= " + parent + " child= " + identifier );
    	if(parent !=null){
    	  level = nodes.get(parent).getLevel();
    	  descendent = nodes.get(parent).getfriendChildCount();
    	}
    	level *= 10;
    	level = level + descendent;
    	Node parentNode =null;
    	 if(parent!=null)
    		parentNode = nodes.get(parent).getParent();
             node = new Node(identifier,level,parentNode);
        nodes.put(identifier, node);

          if (parent != null) {
            nodes.get(parent).addChild(identifier);
          }
    	}
        return node;
    }

    public void display(String identifier) {
        this.display(identifier, ROOT);
    }

    public void display(String identifier, int depth) {
        ArrayList<String> children = nodes.get(identifier).getChildren();

        if (depth == ROOT) {
            System.out.println(nodes.get(identifier).getIdentifier());
        } else {
            String tabs = String.format("%0" + depth + "d", 0).replace("0", "    "); // 4 spaces
            System.out.println(tabs + nodes.get(identifier).getIdentifier() + "=L="+nodes.get(identifier).getLevel());
        }
        depth++;
        for (String child : children) {

            // Recursive call
            this.display(child, depth);
        }
    }

    public Iterator<Node> iterator(String identifier) {
        return this.iterator(identifier, traversalStrategy);
    }

    public Iterator<Node> iterator(String identifier, TraversalStrategy traversalStrategy) {
        return traversalStrategy == TraversalStrategy.DEPTH_FIRST ?
                new BreadthFirstTreeIterator(nodes, identifier) :
                new DepthFirstTreeIterator(nodes, identifier);
    }
}