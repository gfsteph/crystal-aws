require "http"
require "awscr-signer"
require "db/pool"

require "./aws"

# It doesn't handle `Connection: keep-alive` headers :-\
Awscr::Signer::HeaderCollection::BLACKLIST_HEADERS << "connection"

module AWS
  abstract class Client
    macro service_name
      {{SERVICE_NAME}}
    end

    def initialize(
      @access_key_id = AWS.access_key_id,
      @secret_access_key = AWS.secret_access_key,
      @region = AWS.region,
      endpoint_url : String? = nil
    )
      endpoint_scheme = "https" # Default to HTTPS for security
      if endpoint_url
        parsed_endpoint = URI.parse(endpoint_url)
        endpoint_scheme = parsed_endpoint.scheme
        endpoint_host = parsed_endpoint.host.not_nil!
      else
        endpoint_host = "#{service_name}.#{region}.amazonaws.com"
      end
      @endpoint = URI.parse("#{endpoint_scheme}://#{endpoint_host}")

      puts "Listing on #{@endpoint}"
      @signer = Awscr::Signer::Signers::V4.new(service_name, region, access_key_id, secret_access_key)
      @connection_pools = Hash({String, Bool}, DB::Pool(HTTP::Client)).new
    end

    DEFAULT_HEADERS = HTTP::Headers {
      "Connection" => "keep-alive",
      "User-Agent" => "Crystal AWS #{VERSION}",
    }

    protected getter endpoint

    protected def http(host = endpoint.host.not_nil!, tls = endpoint.scheme == "https")
      pool = @connection_pools.fetch({host, tls}) do |key|
        @connection_pools[key] = DB::Pool.new(initial_pool_size: 0, max_idle_pool_size: 20) do
          http = HTTP::Client.new(host, tls: tls)
          http.before_request do |request|
            # Sign request
            request.headers.delete "Authorization"
            request.headers.delete "X-Amz-Content-Sha256"
            request.headers.delete "X-Amz-Date"
            @signer.sign request
          end

          http
        end
      end

      pool.checkout { |http| yield http }
    end
  end
end
