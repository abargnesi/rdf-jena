require File.join(File.dirname(__FILE__), 'spec_helper')
require 'rdf/jena'
require 'rdf/spec/repository'

describe RDF::Jena::Repository do

  # @see lib/rdf/spec/repository.rb in rdf-spec
  it_behaves_like 'an RDF::Repository' do
    let(:repository) { RDF::Jena::Repository.new('test-data') }
  end
end
