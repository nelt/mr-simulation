package org.codingmatters.mr.simulation.graph;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public interface Node {

    static ReducerNode.Builder last() {
        return new ReducerNode.Builder(true);
    }

    static ReducerNode.Builder reducer() {
        return new ReducerNode.Builder(false);
    }

    static MapperNode mapper() {
        return new MapperNode();
    }

    void accept(NodeVisitor visitor);

    class LastReducerNode extends ReducerNode {

        private LastReducerNode(Node[] previousNodes) {
            super(previousNodes);
        }

        @Override
        public void accept(NodeVisitor visitor) {
            visitor.visit(this);
        }

        @Override
        public String toString() {
            return "LastReducerNode{" +
                    "previousNodes=" + Arrays.toString(super.previousNodes) +
                    '}';
        }
    }

    class ReducerNode implements Node {

        @Override
        public void accept(NodeVisitor visitor) {
            visitor.visit(this);
        }

        private final Node[] previousNodes;

        private ReducerNode(Node[] previousNodes) {
            this.previousNodes = previousNodes;
        }

        public Node[] previous() {
            return this.previousNodes;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ReducerNode that = (ReducerNode) o;
            return Arrays.equals(previousNodes, that.previousNodes);
        }

        @Override
        public String toString() {
            return "ReducerNode{" +
                    "previousNodes=" + Arrays.toString(previousNodes) +
                    '}';
        }
    }

    class MapperNode implements Node {
        @Override
        public void accept(NodeVisitor visitor) {
            visitor.visit(this);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            return true;
        }

        @Override
        public String toString() {
            return "MapperNode{}";
        }
    }

    class Builder {
        private final boolean isLast;
        private final List<Node> previous = new LinkedList<>();

        public Builder(boolean isLast) {
            this.isLast = isLast;
        }

        public Builder withPrevious(Node node) {
            this.previous.add(node);
            return this;
        }

        public ReducerNode build() {
            if(this.isLast) {
                return new LastReducerNode(this.previous.toArray(new Node[this.previous.size()]));
            } else {
                return new ReducerNode(this.previous.toArray(new Node[this.previous.size()]));
            }
        }
    }
}
