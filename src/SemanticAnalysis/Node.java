/////////////////////////////////////////////////////////////////////
//  Node.java - Node Structure for Tree                            //
//                                                                 //
//  ver 1.0                                                        //
//  Language:      Eclipse , Java                                  //
//  Platform:      Dell, Windows 8.1                               //
//  Application:   Semantic Analysis using Map Reduce              //
//  Author:		   Ankur Pandey , Nisha Choudhary                  //
/////////////////////////////////////////////////////////////////////
package SemanticAnalysis;

import java.util.ArrayList;


/////////////////////////////////////////////////////////////////////
/// Breath First Tree Traversal 
/////////////////////////////////////////////////////////////////////
public class Node {
	private String identifier;
	private ArrayList<String> children;
	Integer Level;
	Node parent; /// Stores Parent pointer to node
	private Integer friendChildCount; // stores friend count
   
    //Constructor
    ////////////////////////////////////////////////////////////////
	public Integer getfriendChildCount(){
		return children.size();
	}
    // returns Parent
    ////////////////////////////////////////////////////////////////
	public Node getParent() {
		return parent;
	}
    // Returns Level
    ////////////////////////////////////////////////////////////////
	public Integer getLevel() {
		return Level;
	}
	// node constructor
	////////////////////////////////////////////////////////////////
	public Node(String identifier,Integer  level,Node parent_) {
		this.identifier = identifier;
		Level= level;
		parent = parent_;
		children = new ArrayList<String>();
	}

	//Properties
	////////////////////////////////////////////////////////////////
	public String getIdentifier() {
		return identifier;
	}
	//Returns Children
	///////////////////////////////////////////////////////////////
	public ArrayList<String> getChildren() {
		return children;
	}
	// Addes child
	////////////////////////////////////////////////////////////////
	public void addChild(String identifier) {
		children.add(identifier);
	}
}