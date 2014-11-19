require 'net/https'
require 'json'

class Sumologic
  class ApiError < RuntimeError; end

  def self.collector_exists?(node_name, email, pass, api_timeout = nil)
    collector = Sumologic::Collector.new(
      name: node_name,
      api_username: email,
      api_password: pass
    )
    collector.exist?(api_timeout)
  end

  class Collector
    attr_reader :name, :api_username, :api_password

    def initialize(opts = {})
      @name = opts[:name]
      @api_username = opts[:api_username]
      @api_password = opts[:api_password]
    end

    def api_endpoint
      'https://api.sumologic.com/api/v1'
    end

    def sources
      @sources ||= fetch_source_data
    end

    def metadata(api_timeout = nil)
      collectors(api_timeout)['collectors'].find { |c|c['name'] == name }
    end

    def exist?(api_timeout = nil)
      !!metadata(api_timeout)
    end

    def api_request(options = {})
      uri = options[:uri]
      request = options[:request]
      timeout_secs = 0
      timeout_secs = options[:api_timeout] unless options[:api_timeout].nil?
      parse_json = if options.has_key?(:parse_json)
                     options[:parse_json]
                   else
                     true
                   end
      http = Net::HTTP.new(uri.host, uri.port)
      http.use_ssl = true
      request.basic_auth(api_username, api_password)
      if timeout_secs != 0
        response = nil
        Timeout.timeout(timeout_secs) do
          sleep_to = 0
          begin
            response = http.request(request)
            raise ApiError, "Unable to get source list #{response.inspect}" unless response.is_a?(Net::HTTPSuccess)
          rescue
            Chef::Log.warn("Sumologic api timedout... retrying in #{sleep_to}s")
            sleep sleep_to
            sleep_to += 10
            retry
          end
        end
      else
        response = http.request(request)
        raise ApiError, "Unable to get source list #{response.inspect}" unless response.is_a?(Net::HTTPSuccess)
      end

      if parse_json
        begin
          JSON.parse(response.body)
        rescue JSON::ParserError => e
          Chef::Log.warn('Sumlogic sent something that does not appear to be JSON, here it is...')
          Chef::Log.warn("status code: #{response.code}")
          Chef::Log.warn(response.body)
          raise e
        end
      else
        response
      end
    end

    def refresh!
      @collectors ||= list_collectors
      @sources = fetch_source_data
      nil
    end

    def list_collectors(api_timeout = nil)
      uri = URI.parse(api_endpoint + '/collectors')
      request = Net::HTTP::Get.new(uri.request_uri)
      api_request(uri: uri, request: request, api_timeout: api_timeout)
    end

    def collectors(api_timeout = nil)
      @collectors ||= list_collectors(api_timeout)
    end

    def id
      metadata['id']
    end

    def fetch_source_data(api_timeout = nil)
      u = URI.parse(api_endpoint + "/collectors/#{id}/sources")
      request = Net::HTTP::Get.new(u.request_uri)
      details = api_request(uri: u, request: request, api_timeout: api_timeout)
      details['sources']
    end

    def source_exist?(source_name)
      sources.any? { |c| c['name'] == source_name }
    end

    def source(source_name)
      sources.find { |c| c['name'] == source_name }
    end

    def add_source!(source_data, api_timeout = nil)
      u = URI.parse(api_endpoint + "/collectors/#{id}/sources")
      request = Net::HTTP::Post.new(u.request_uri)
      request.body = JSON.dump({ source: source_data })
      request.content_type = 'application/json'
      response = api_request(uri: u, request: request, parse_json: false, api_timeout: api_timeout)
      response
    end

    def delete_source!(source_id, api_timeout = nil)
      u = URI.parse(api_endpoint + "/collectors/#{source_id}")
      request = Net::HTTP::Delete.new(u.request_uri)
      response = api_request(uri: u, request: request, parse_json: false, api_timeout: api_timeout)
      response
    end

    def update_source!(source_id, source_data, api_timeout = nil)
      u = URI.parse("https://api.sumologic.com/api/v1/collectors/#{id}/sources/#{source_id}")
      request = Net::HTTP::Put.new(u.request_uri)
      request.body = JSON.dump({ source: source_data.merge(id: source_id) })
      request.content_type = 'application/json'
      request['If-Match'] = get_etag(source_id)
      response = api_request(uri: u, request: request, parse_json: false, api_timeout: api_timeout)
      response
    end

    def get_etag(source_id, api_timeout = nil)
      u = URI.parse("https://api.sumologic.com/api/v1/collectors/#{id}/sources/#{source_id}")
      request = Net::HTTP::Get.new(u.request_uri)
      response = api_request(uri: u, request: request, parse_json: false, api_timeout: api_timeout)
      response['etag']
    end
  end
end
