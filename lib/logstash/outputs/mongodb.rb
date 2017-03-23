# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "mongo"
require_relative "bson/big_decimal"
require_relative "bson/logstash_timestamp"

# This output writes events to MongoDB.
class LogStash::Outputs::Mongodb < LogStash::Outputs::Base

  config_name "mongodb"

  # A MongoDB URI to connect to.
  # See http://docs.mongodb.org/manual/reference/connection-string/.
  config :uri, :validate => :string, :required => true

  # The database to use.
  config :database, :validate => :string, :required => false

  # The collection to use. This value can use `%{foo}` values to dynamically
  # select a collection based on data in the event.
  config :collection, :validate => :string, :required => false

  # The number of seconds to wait after failure before retrying.
  config :retry_delay, :validate => :number, :default => 3, :required => false

  # If true, an "_id" field will be added to the document before insertion.
  # The "_id" field will use the timestamp of the event and overwrite an existing
  # "_id" field in the event.
  config :generateId, :validate => :boolean, :default => false

  public
  def register
    Mongo::Logger.logger = @logger
    conn = Mongo::Client.new(@uri)
    @db = conn
  end # def register

  def receive(event)
    begin
      # Our timestamp object now has a to_bson method, using it here
      # {}.merge(other) so we don't taint the event hash innards
      document = {}.merge(event.to_hash)

      # Need to convert plain string values to respective Object Types
      puts document['post_created_at'] 
      document['post_created_at'] = DateTime.parse(document['post_created_at'])

      document["_id"] = "#{document["network"]}-#{document["social_source_uid"]}" 

      document["social_monitor_sources"].each do |doc|
        if doc["company_uid"].empty?
          puts document
        end

        unless doc['keywords'].nil?
          doc['keywords'].each do |keyword|
            keyword[0].gsub!("$", "\\u0024") if keyword[0].start_with?("$")
            keyword[0].gsub!(".", "\\u002e") if keyword[0].start_with?(".")
          end
        end

        @db.use(doc["company_uid"])[event.sprintf(doc["monitor_uid"])].insert_one(document)
        @db.use(doc["company_uid"])[event.sprintf(doc["monitor_uid"])].indexes.create_one({"post_created_at" => 1})
      end

    rescue => e
      p e.to_json
      @logger.warn("Failed to send event to MongoDB", :event => event, :exception => e,
                   :backtrace => e.backtrace)
    end
  end # def receive
end # class LogStash::Outputs::Mongodb
