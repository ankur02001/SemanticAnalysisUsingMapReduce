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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;


/////////////////////////////////////////////////////////////////////
///Depth First Tree Traversal 
/////////////////////////////////////////////////////////////////////
public class DepthFirstTreeIterator implements Iterator<Node> {
    private LinkedList<Node> list;

    public DepthFirstTreeIterator(HashMap<String, Node> tree, String identifier) {
        list = new LinkedList<Node>();

        if (tree.containsKey(identifier)) {
            this.buildList(tree, identifier);
        }
    }
    
    private void buildList(HashMap<String, Node> tree, String identifier) {
        list.add(tree.get(identifier));
        ArrayList<String> children = tree.get(identifier).getChildren();
        for (String child : children) {

            // Recursive call
            this.buildList(tree, child);
        }
    }

    @Override
    public boolean hasNext() {
        return !list.isEmpty();
    }

    @Override
    public Node next() {
        return list.poll();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}