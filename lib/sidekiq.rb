# encoding: utf-8
require 'sidekiq/version'
require 'sidekiq/logging'
require 'sidekiq/client'
require 'sidekiq/worker'
require 'sidekiq/redis_connection'
require 'sidekiq/util'
require 'sidekiq/api'

require 'json'

module Sidekiq
  NAME = "Sidekiq"
  LICENSE = 'See LICENSE and the LGPL-3.0 for licensing details.'

  DEFAULTS = {
    :queues => [],
    :concurrency => 25,
    :require => '.',
    :environment => nil,
    :timeout => 8,
    :profile => false,
  }

  def self.❨╯°□°❩╯︵┻━┻
    puts "Calm down, bro"
  end

  def self.options
    @options ||= DEFAULTS.dup
  end

  def self.options=(opts)
    @options = opts
  end

  ##
  # Configuration for Sidekiq server, use like:
  #
  #   Sidekiq.configure_server do |config|
  #     config.redis = { :namespace => 'myapp', :size => 25, :url => 'redis://myhost:8877/0' }
  #     config.server_middleware do |chain|
  #       chain.add MyServerHook
  #     end
  #   end
  def self.configure_server
    yield self if server?
  end

  ##
  # Configuration for Sidekiq client, use like:
  #
  #   Sidekiq.configure_client do |config|
  #     config.redis = { :namespace => 'myapp', :size => 1, :url => 'redis://myhost:8877/0' }
  #   end
  def self.configure_client
    yield self unless server?
  end

  def self.server?
    defined?(Sidekiq::CLI)
  end

  def self.redis(fetcher = nil, &block)
    raise ArgumentError, "requires a block" if !block

    if @hashes
      @multi_redis ||= {}

      if fetcher
        if @multi_redis[fetcher].nil?
          if config_hash = @hashes.select { |h| h[:fetcher].nil? }.shift
            @multi_redis[fetcher] = Sidekiq::RedisConnection.create(config_hash)
            config_hash[:fetcher] = fetcher
          end
        end
        @multi_redis[fetcher].with(&block)
      else
        @fallback ||= Sidekiq::RedisConnection.create(@hashes.first || {})
        @fallback.with(&block)
      end

    else
      @redis ||= Sidekiq::RedisConnection.create(@hash || {})
      @redis.with(&block)
    end
  end

  def self.redis=(arg)
    return @redis = arg if arg.is_a?(ConnectionPool)

    case arg
    when Hash
      @hash = arg
    when Array
      @hashes = arg
    else
      raise ArgumentError, "redis= requires an Array, Hash, or ConnectionPool"
    end
  end

  def self.multi_redis?
    !!@hashes
  end

  def self.fetcher_class
    Sidekiq.multi_redis? ? Sidekiq::FetcherPool : Sidekiq::Fetcher
  end

  def self.fetcher_pool_count
    @hashes.nil? ? 1 : @hashes.length
  end

  def self.client_middleware
    @client_chain ||= Client.default_middleware
    yield @client_chain if block_given?
    @client_chain
  end

  def self.server_middleware
    @server_chain ||= Processor.default_middleware
    yield @server_chain if block_given?
    @server_chain
  end

  def self.default_worker_options=(hash)
    @default_worker_options = default_worker_options.merge(hash)
  end

  def self.default_worker_options
    @default_worker_options || { 'retry' => true, 'queue' => 'default' }
  end

  def self.load_json(string)
    JSON.parse(string)
  end

  def self.dump_json(object)
    JSON.generate(object)
  end

  def self.logger
    Sidekiq::Logging.logger
  end

  def self.logger=(log)
    Sidekiq::Logging.logger = log
  end

  def self.poll_interval=(interval)
    self.options[:poll_interval] = interval
  end

end

require 'sidekiq/extensions/class_methods'
require 'sidekiq/extensions/action_mailer'
require 'sidekiq/extensions/active_record'
require 'sidekiq/rails' if defined?(::Rails::Engine)
