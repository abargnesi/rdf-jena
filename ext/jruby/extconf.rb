# Check that Java and JRuby requirements are met.
#
# Requirements
# - JRuby as the current ruby engine.
# - Java 8.

# Definitions

JRUBY_MESSAGE = %Q{
The rdf-jena gem only runs on JRuby due to dependency on Apache Jena.

Requirements
- Java 8 (http://www.oracle.com/technetwork/java/javase/downloads/)
- JRuby 1.7 or 9k series (http://jruby.org/download)

Steps
- Install Java 8 or set as the default Java for your system.
- Install JRuby.
- Run "gem install rdf-jena" using JRuby.
}

JAVA8_MESSAGE = %Q{
The rdf-jena gem only runs on Java 8 due to dependency on Apache Jena 3.x series.

Requirements
- Java 8 (http://www.oracle.com/technetwork/java/javase/downloads/)
- JRuby 1.7 or 9k series (http://jruby.org/download)

Steps
- Install Java 8 or set as the default Java for your system.
- Run "gem install rdf-jena" using JRuby.
}

def running_jruby?
  RUBY_ENGINE =~ /^jruby/
end

def running_java8?
  return false if not running_jruby?

  java_import 'java.lang.System'
  java_version = System.get_property('java.version')

  return java_version =~ /^#{Regexp.escape('1.8.0')}/
end

# Checks

if not running_jruby?
  $stderr.puts JRUBY_MESSAGE
  exit 10
end

if not running_java8?
  $stderr.puts JAVA8_MESSAGE
  exit 20
end

# Checks passed, return empty success Makefile to tell JRuby that
# all is well.
File.open('Makefile', 'w') do |f|
  f.puts "all:"
  f.puts "\t@true"
  f.puts "install:"
  f.puts "\t@true"
end
exit 0
