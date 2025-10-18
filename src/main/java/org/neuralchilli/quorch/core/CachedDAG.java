package org.neuralchilli.quorch.core;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * Cached DAG wrapper for Hazelcast storage.
 * Caches compiled DAGs to avoid rebuilding them on every evaluation.
 *
 * Performance optimization: A graph with 100 tasks would otherwise rebuild
 * the DAG 100 times (once per task completion). With caching, we build once
 * and reuse.
 *
 * Must be a plain class (not record) for Hazelcast serialization.
 */
public final class CachedDAG implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final DirectedAcyclicGraph<TaskNode, DefaultEdge> dag;

    public CachedDAG(DirectedAcyclicGraph<TaskNode, DefaultEdge> dag) {
        if (dag == null) {
            throw new IllegalArgumentException("DAG cannot be null");
        }
        this.dag = dag;
    }

    public DirectedAcyclicGraph<TaskNode, DefaultEdge> dag() {
        return dag;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        CachedDAG that = (CachedDAG) obj;
        return Objects.equals(dag, that.dag);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dag);
    }

    @Override
    public String toString() {
        return "CachedDAG[vertices=" + dag.vertexSet().size() +
                ", edges=" + dag.edgeSet().size() + "]";
    }
}