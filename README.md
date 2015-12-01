# rdf-jena

RDF Storage backed by Apache Jena and running on JRuby.

Works on JRuby 1.7 and 9k series.

## Install

`gem install rdf-jena`

## Use

```ruby
require 'rdf/jena'

# Create TDB directory 'data'.
r = RDF::Jena::Repository.new('data')

# Insert a statement into the default graph (Jena's default model for the TDB dataset).
r << RDF::Statement.new(
  RDF::URI('https://github.com/abargnesi'),
  RDF::URI('http://purl.org/dc/terms/created'),
  RDF::URI('https://github.com/abargnesi/rdf-jena')
)

# Work with graphs.

  # Create repository data source for graph.
  graph_repository = RDF::Repository.new(:graph_name=>'https://github.com')
  graph_repository << RDF::Statement.new(
    RDF::URI('https://github.com/abargnesi/rdf-jena'),
    RDF::URI('http://purl.org/dc/terms/requires'),
    RDF::URI('https://github.com/ruby-rdf/rdf')
  )

  # Create named graph from repository.
  graph = RDF::Graph.new('https://github.com', :data => graph_repository)

  # Insert as a named graph into repository (Added as a named model to the TDB dataset).
  r.insert_graph(graph)

  # You can also:
  
    # replace a graph
    r.replace_graph(graph)

    # delete a graph
    r.delete_graph(graph)
```
