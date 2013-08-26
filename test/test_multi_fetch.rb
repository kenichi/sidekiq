require 'helper'
require 'sidekiq/fetch'

# NOTE - these tests require a redis running on both the default port 6379 and
# another on port 6380. they should be completely independent of each other.

class TestMultiFetch < Minitest::Test

  describe 'multi_fetcher' do

    before do
      Sidekiq.redis = [
        {:namespace => 'fuzzy'},
        {:url => 'redis://127.0.0.1:6380', :namespace => 'fuzzy'}
      ]
      [0,1].each do |n|
        Sidekiq.redis(n) do |conn|
          conn.flushdb
          conn.rpush('queue:basic', "msg_#{n}")
        end
      end
    end

    it 'retreives from each redis' do
      fetches = [0,1].map { |n| Sidekiq::MultiFetch.new(:fetcher => n, :queues => ['basic', 'bar']) }
      uow = nil
      [0,1].each do |n|
        uow = fetches[n].retrieve_work
        refute_nil uow
        assert_equal 'basic', uow.queue_name
        assert_equal "msg_#{n}", uow.message
      end
      q = Sidekiq::Queue.new('basic')
      assert_equal 0, q.size
      uow.requeue
      assert_equal 1, q.size
      assert_nil uow.acknowledge
      Sidekiq.redis(1) do |conn|
        assert_equal 0, conn.llen('queue:basic')
      end
    end

    it 'retrieves with strict setting' do
      fetch = Sidekiq::MultiFetch.new(:queues => ['basic', 'bar', 'bar'], :strict => true)
      cmd = fetch.queues_cmd
      assert_equal cmd, ['queue:basic', 'queue:bar', 1]
    end

    it 'bulk requeues' do
      q1 = Sidekiq::Queue.new('foo')
      q2 = Sidekiq::Queue.new('bar')
      assert_equal 0, q1.size
      assert_equal 0, q2.size
      uow = Sidekiq::MultiFetch::UnitOfWork
      Sidekiq::MultiFetch.bulk_requeue([uow.new('fuzzy:queue:foo', 'bob'), uow.new('fuzzy:queue:foo', 'bar'), uow.new('fuzzy:queue:bar', 'widget')])
      assert_equal 2, q1.size
      assert_equal 1, q2.size
    end

    after do
      # clear things out for other, non-multi-redis, tests
      Sidekiq.instance_variable_set :@hashes, nil
    end

  end
end
