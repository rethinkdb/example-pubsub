Gem::Specification.new do |s|
  s.name = 'repubsub'
  s.version = '1.0.1'
  s.summary = "pubsub on RethinkDB"
  s.summary = "A publish-subscribe library using RethinkDB"
  s.homepage = "http://rethinkdb.com/docs/publish-subscribe/ruby/"
  s.files = ["repubsub.rb"]
  s.authors = ["Josh Kuhn"]
  s.email = "josh@rethinkdb.com"
  s.license = "MIT"
  s.add_runtime_dependency 'rethinkdb', '>= 1.13'
end
