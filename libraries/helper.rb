require 'net/https'
require 'json'

class Sumologic
  class ApiError < RuntimeError; end

  def self.collector_exists?(node_name, email, pass, api_collectors_limit)
    collector = Sumologic::Collector.new(
      :name => node_name,
      :api_username => email,
      :api_password => pass,
      :api_collectors_limit => api_collectors_limit
    )
    collector.exist?
  end

  class Collector
    attr_reader :name, :api_username, :api_password, :api_collectors_limit

    def initialize(opts = {})
      @name = opts[:name]
      @api_username = opts[:api_username]
      @api_password = opts[:api_password]
      @api_endpoint = opts[:api_endpoint] || 'https://api.sumologic.com/api/v1'
      @api_collectors_limit = opts[:api_collectors_limit]
    end

    def api_endpoint
      @api_endpoint
    end

    def sources
      @sources ||= fetch_source_data
    end

    def metadata
      collectors['collectors'].select { |c|c['name'] == name || c['name'] =~ /#{name}-[0-9]{13}/ }.first
    end

    def exist?
      !metadata.nil?
    end

    def api_request(options = {})
      uri = options[:uri]
      request = options[:request]
      parse_json = if options.has_key?(:parse_json)
                     options[:parse_json]
                   else
                     true
                   end
      http = Net::HTTP.new(uri.host, uri.port)
      http.use_ssl = true
      request.basic_auth(api_username, api_password)
      response = http.request(request)

      raise ApiError, "Unable to get source list #{response.inspect}" unless response.is_a?(Net::HTTPSuccess)
      if parse_json
        JSON.parse(response.body)
      else
        response
      end
    end

    def refresh!
      @collectors ||= list_collectors
      @sources = fetch_source_data
      nil
    end

    def list_collectors
      uri = URI.parse(api_endpoint + '/collectors?limit=' + api_collectors_limit.to_s)
      request = Net::HTTP::Get.new(uri.request_uri)
      api_request(:uri => uri, :request => request)
    end

    def collectors
      @collectors ||= list_collectors
    end

    def id
      metadata['id']
    end

    def fetch_source_data
      u = URI.parse(api_endpoint + "/collectors/#{id}/sources")
      request = Net::HTTP::Get.new(u.request_uri)
      details = api_request(:uri => u, :request => request)
      details['sources']
    end

    def source_exist?(source_name)
      exist? && sources.any? { |c| c['name'] == source_name }
    end

    def source(source_name)
      sources.select { |c| c['name'] == source_name }.first
    end

    def add_source!(source_data)
      u = URI.parse(api_endpoint + "/collectors/#{id}/sources")
      request = Net::HTTP::Post.new(u.request_uri)
      request.body = JSON.dump({ :source => source_data })
      request.content_type = 'application/json'
      response = api_request(:uri => u, :request => request, :parse_json => false)
      response
    end

    def delete_source!(source_id)
      u = URI.parse(api_endpoint + "/collectors/#{source_id}")
      request = Net::HTTP::Delete.new(u.request_uri)
      response = api_request(:uri => u, :request => request, :parse_json => false)
      response
    end

    def update_source!(source_id, source_data)
      u = URI.parse(api_endpoint + "/collectors/#{id}/sources/#{source_id}")
      request = Net::HTTP::Put.new(u.request_uri)
      request.body = JSON.dump({ :source => source_data.merge(:id => source_id) })
      request.content_type = 'application/json'
      request['If-Match'] = get_etag(source_id)
      response = api_request(:uri => u, :request => request, :parse_json => false)
      response
    end

    def get_etag(source_id)
      u = URI.parse(api_endpoint + "/collectors/#{id}/sources/#{source_id}")
      request = Net::HTTP::Get.new(u.request_uri)
      response = api_request(:uri => u, :request => request, :parse_json => false)
      response['etag']
    end
  end
end
