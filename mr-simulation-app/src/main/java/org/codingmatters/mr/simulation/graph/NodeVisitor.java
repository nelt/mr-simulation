package org.codingmatters.mr.simulation.graph;

public interface NodeVisitor {
    void visit(Node.MapperNode node);
    void visit(Node.ReducerNode node);
    void visit(Node.LastReducerNode node);
}
