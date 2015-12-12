# Change Log

All notable changes to rdf-jena will be documented in this file. The curated log begins at changes to version 0.1.0.

This project adheres to [Semantic Versioning](http://semver.org/).

## [0.3.3][0.3.3] - 2015-12-11
### Fixed
- Return false if RDF statement is nil for "insert_statement" API.
- Detect RDF format from filename using Jena. Fallback to NQUADS.

## [0.3.2][0.3.2] - 2015-12-10
### Fixed
- Fixed query / delete from default graph using RDF::Jena::Repository.

## [0.3.1][0.3.1] - 2015-12-08
### Fixed
- Fixed RDF::Jena::Repository to store statements in the correct graph. If a graph_name is not provided the default graph for the Jena dataset is used.
- Fixed has_statement? and query_pattern to support nilable s,p,o,c values.

### Added
- Added insert_file to insert a path without having to create an RDF::Reader.

## [0.3.0][0.3.0] - 2015-12-07
### Changed
- RDF::Jena::Repository now acts like a Jena DatasetGraph. This can be treated like a quad store as well as individual graphs (i.e. each_graph).
- RDF::Jena::Graph now acts like a Jena Graph.

### Added
- Repository/Graph now implement insert_statements(enumerator) and insert_reader(IO). The insert_reader only takes IO at the moment and assumes NQUADS for Repository, NTRIPLES from Graph.
- Repository provides the 'graph_size' method to return number of graphs. The 'size' method will return number of quads in the Repository.

## [0.2.1][0.2.1] - 2015-12-03
### Added
- Included install-time checks that JRuby is running on Java 8.

## 0.2.0 - 2015-12-01
### Fixed
- Alias `each_statement` to `each`. The java method iterateStatements is now provided as both ruby methods.

### Added
- Implemented query_pattern method to take advantage of Apache Jena model selectors.

## 0.1.0 - 2015-12-01
### Added
- Implements an [RDF.rb][RDF.rb] RDF::Repository using Apache Jena with TDB storage.
- Implements an [RDF.rb][RDF.rb] RDF::Graph as a Jena named model.
- Implemented `each_statement` to enumerate all statements in the repository (TDB Dataset's defaut model).
- Implemented `has_statement?` to check for presence of statement in repository or graph.
- Implemented mutation methods `clear_statements`, `delete_statement`, and `insert_statement`.
- Support for query and modification of graphs using `each_graph`, `insert_graph`, `replace_graph`, and `delete_graph`.
- Supporting boolean flag on RDF::Repository to union named graphs with the default when retrieved (see `each_graph`).

[RDF.rb]:   https://github.com/ruby-rdf/rdf
[0.3.3]:    https://github.com/abargnesi/rdf-jena/compare/0.3.2...0.3.3
[0.3.2]:    https://github.com/abargnesi/rdf-jena/compare/0.3.1...0.3.2
[0.3.1]:    https://github.com/abargnesi/rdf-jena/compare/0.3.0...0.3.1
[0.3.0]:    https://github.com/abargnesi/rdf-jena/compare/0.2.1...0.3.0
[0.2.1]:    https://github.com/abargnesi/rdf-jena/compare/0.2.0...0.2.1
