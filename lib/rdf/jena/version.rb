module RDF
  module Jena

    module Version

      VERSION_FILE = File.join(File.expand_path(File.dirname(__FILE__)), '..', '..', '..', 'VERSION')
      MAJOR, MINOR, TINY, EXTRA = File.read(VERSION_FILE).chomp.split('.')
      STRING = [MAJOR, MINOR, TINY, EXTRA].compact.join('.')

      # Returns version string.
      #
      # @return [String]
      def self.to_s() STRING end

      # Returns version string.
      #
      # @return [String]
      def self.to_str() STRING end

      # Returns version {Array}.
      #
      # @return [Array(Integer, Integer, Integer)]
      def self.to_a() [MAJOR, MINOR, TINY] end
    end
  end
end
