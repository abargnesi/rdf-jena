package com.github.rdf_jena;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Selector;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.jruby.*;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.exceptions.RaiseException;
import org.jruby.runtime.Block;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

import java.io.InputStream;
import java.util.Optional;

import static com.github.rdf_jena.JenaConverters.convertRDFResource;
import static com.github.rdf_jena.JenaConverters.convertRDFStatementToSelector;
import static com.github.rdf_jena.JenaConverters.convertRDFTriple;
import static com.github.rdf_jena.RubyRDFConverters.convertTriple;
import static com.github.rdf_jena.TransactionUtil.executeInTransaction;
import static org.jruby.RubyBoolean.newBoolean;
import static org.jruby.RubyFixnum.newFixnum;
import static org.jruby.RubySymbol.newSymbol;

@JRubyClass(name = "Graph")
public class Graph extends RubyObject {

    public static ObjectAllocator Allocator = Graph::new;

    // ruby state
    protected IRubyObject                 graphName;

    // java state
    protected Repository                  repository;
    protected Dataset                     ds;
    protected org.apache.jena.graph.Graph graph;

    public Graph(Ruby runtime, RubyClass metaclass) {
        super(runtime, metaclass);
    }

    // Ruby methods
    @JRubyMethod(name = "initialize", required = 2)
    public IRubyObject initialize(
            ThreadContext ctx,
            IRubyObject graphName,
            IRubyObject repository
    ) {
        this.repository = (Repository) repository.toJava(Repository.class);
        this.ds = this.repository.ds;

        if (graphName.isNil() || graphName.eql(newSymbol(ctx.runtime, "default"))) {
            // establish this as the default graph
            this.graphName = newSymbol(ctx.runtime, "default");
            this.graph     = ds.asDatasetGraph().getDefaultGraph();
        } else {
            // establish this as a named graph, if it is exists
            String javaGraphName = graphName.asString().asJavaString();
            this.graph     = ds.asDatasetGraph().getGraph(NodeFactory.createURI(javaGraphName));
            if (this.graph == null) {
                throw ctx.runtime.newArgumentError("graph does not exist in repository");
            }

            this.graphName = graphName;
        }

        return ctx.nil;
    }

    @JRubyMethod(name = "graph_name")
    public IRubyObject getGraphName(ThreadContext ctx) {
        if (graphName == null) {
            return ctx.nil;
        }

        return graphName;
    }

    @JRubyMethod(name = "data")
    public IRubyObject getData(ThreadContext ctx) {
        return iterateStatements(ctx, Block.NULL_BLOCK);
    }

    @JRubyMethod(name = "durable?")
    public RubyBoolean isDurable(ThreadContext ctx) {
        return newBoolean(ctx.runtime, true);
    }

    @JRubyMethod(name = "empty?")
    public RubyBoolean isEmpty(ThreadContext ctx) {
        return executeInTransaction(ds, ReadWrite.READ, ds -> newBoolean(ctx.runtime, graph.isEmpty()));
    }

    @JRubyMethod(name = {"count", "size"})
    public RubyFixnum size(ThreadContext ctx) {
        return executeInTransaction(ds, ReadWrite.READ, ds -> newFixnum(ctx.runtime, graph.size()));
    }

    @JRubyMethod(name = {"each_statement", "each"})
    public IRubyObject iterateStatements(ThreadContext ctx, Block block) {
        if (block != Block.NULL_BLOCK) {
            executeInTransaction(ds, ReadWrite.READ, ds -> {
                ExtendedIterator<Triple> triples = graph.find(null, null, null);
                while (triples.hasNext()) {
                    block.call(ctx, convertTriple(ctx, triples.next()));
                }
                return null;
            });
            return ctx.nil;
        } else {
            return callMethod(ctx, "enum_statement");
        }
    }

    @JRubyMethod(name = {"query_pattern"}, required = 1, optional = 1)
    public IRubyObject queryPattern(ThreadContext ctx, IRubyObject[] args, Block block) {
        if (block != Block.NULL_BLOCK) {
            IRubyObject pattern = args[0];

            // argument 1; unused options

            executeInTransaction(ds, ReadWrite.READ, ds -> {
                Selector selector = convertRDFStatementToSelector(ctx, pattern);
                ExtendedIterator<Triple> triples;
                if (selector == null) {
                    triples = graph.find(null, null, null);
                } else {
                    triples = graph.find(
                            Optional.ofNullable(selector.getSubject()).map(RDFNode::asNode).orElse(null),
                            Optional.ofNullable(selector.getPredicate()).map(RDFNode::asNode).orElse(null),
                            Optional.ofNullable(selector.getObject()).map(RDFNode::asNode).orElse(null)
                    );
                }
                while (triples.hasNext()) {
                    block.call(ctx, convertTriple(ctx, triples.next()));
                }
                return null;
            });
            return ctx.nil;
        } else {
            IRubyObject[] enumArgs = new IRubyObject[args.length+1];
            enumArgs[0] = newSymbol(ctx.runtime, "query_pattern");
            System.arraycopy(args, 0, enumArgs, 1, args.length);
            return callMethod(ctx, "enum_for", enumArgs);
        }
    }

    @JRubyMethod(name = "has_statement?", required = 1)
    public RubyBoolean hasStatement(ThreadContext ctx, IRubyObject rdfStatement) {
        if (rdfStatement == null) {
            return newBoolean(ctx.runtime, false);
        }

        return executeInTransaction(ds, ReadWrite.READ, ds -> {
            Triple triple = convertRDFTriple(ctx, rdfStatement);
            return newBoolean(ctx.runtime, graph.contains(triple));
        });
    }

    @JRubyMethod(name = "insert_statements", required = 1)
    public RubyBoolean insertStatements(ThreadContext ctx, IRubyObject rdfStatements) {
        if (rdfStatements == null) {
            return newBoolean(ctx.runtime, false);
        }

        return executeInTransaction(ds, ReadWrite.WRITE, ds -> {
            RubyEnumerator statementEnumerator = (RubyEnumerator) rdfStatements.callMethod(ctx, "each");
            try {
                while (true) {
                    RubyArray array = statementEnumerator.next(ctx).convertToArray();
                    Node subject   = Optional.ofNullable(convertRDFResource(ctx, array.entry(0))).map(RDFNode::asNode).orElse(null);
                    Node predicate = Optional.ofNullable(convertRDFResource(ctx, array.entry(1))).map(RDFNode::asNode).orElse(null);
                    Node object    = Optional.ofNullable(convertRDFResource(ctx, array.entry(2))).map(RDFNode::asNode).orElse(null);
                    graph.add(new Triple(subject, predicate, object));
                }
            } catch (RaiseException ex) {
                // Handle StopIteration as a terminator for iterating a Ruby Enumerator.
                // All other exceptions are rethrown.
                if (ex.getException().getMetaClass() == ctx.runtime.getStopIteration()) {
                    // safe to skip
                } else {
                    // need to rethrow, this is truly an exception
                    throw ex;
                }
            }
            return newBoolean(ctx.runtime, true);
        });
    }

    @JRubyMethod(name = "insert_statement", required = 1)
    public IRubyObject insertStatement(ThreadContext ctx, IRubyObject rdfStatement) {
        if (rdfStatement == null) {
            return newBoolean(ctx.runtime, false);
        }

        return executeInTransaction(ds, ReadWrite.WRITE, ds -> {
            Triple triple = convertRDFTriple(ctx, rdfStatement);

            if (graph.contains(triple)) {
                return newBoolean(ctx.runtime, false);
            }

            graph.add(triple);
            return newBoolean(ctx.runtime, true);
        });
    }

    @JRubyMethod(name = "insert_reader", required = 1)
    public RubyBoolean insertReader(ThreadContext ctx, IRubyObject reader) {
        RubyFile file      = ((RubyFile) reader);
        InputStream stream = file.getInStream();

        return executeInTransaction(ds, ReadWrite.WRITE, ds -> {
            RDFDataMgr.read(graph, stream, Lang.NTRIPLES);
            return newBoolean(ctx.runtime, true);
        });
    }

    @JRubyMethod(name = "clear_statements")
    public IRubyObject clearStatements(ThreadContext ctx) {
        executeInTransaction(ds, ReadWrite.WRITE, ds -> {
            graph.clear();
            return null;
        });
        return ctx.nil;
    }


    @JRubyMethod(name = "delete_statement", required = 1)
    public IRubyObject deleteStatement(ThreadContext ctx, IRubyObject rdfStatement) {
        if (rdfStatement == null) {
            return ctx.nil;
        }

        executeInTransaction(ds, ReadWrite.WRITE, ds -> {
            Triple triple = convertRDFTriple(ctx, rdfStatement);
            graph.remove(
                    Optional.ofNullable(triple).map(Triple::getSubject).orElse(null),
                    Optional.ofNullable(triple).map(Triple::getPredicate).orElse(null),
                    Optional.ofNullable(triple).map(Triple::getObject).orElse(null)
            );
            return null;
        });
        return ctx.nil;
    }
}
