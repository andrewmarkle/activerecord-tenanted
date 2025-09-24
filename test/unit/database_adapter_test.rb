# frozen_string_literal: true

require "test_helper"

describe ActiveRecord::Tenanted::DatabaseAdapter do
  let(:sqlite_config) { create_config("sqlite3", "test.sqlite3") }

  describe ".adapter_for" do
    test "selects correct adapter for sqlite3" do
      adapter = ActiveRecord::Tenanted::DatabaseAdapter.adapter_for(sqlite_config)
      assert_instance_of ActiveRecord::Tenanted::DatabaseAdapters::SQLite, adapter
    end

    test "raises error for unsupported adapter" do
      unsupported_config = create_config("mongodb", "test_db")

      error = assert_raises ActiveRecord::Tenanted::Error do
        ActiveRecord::Tenanted::DatabaseAdapter.adapter_for(unsupported_config)
      end

      assert_includes error.message, "Unsupported database adapter for tenanting: mongodb. Supported adapters: sqlite3"
    end
  end

  describe "delegation" do
    test ".create_database calls SQLite#create_database" do
      sqlite_mock = Minitest::Mock.new
      sqlite_mock.expect(:create_database, nil)

      ActiveRecord::Tenanted::DatabaseAdapters::SQLite.stub(:new, sqlite_mock) do
        ActiveRecord::Tenanted::DatabaseAdapter.create_database(sqlite_config)
      end

      assert_mock sqlite_mock
    end

    test ".drop_database calls SQLite#drop_database" do
      sqlite_mock = Minitest::Mock.new
      sqlite_mock.expect(:drop_database, nil)

      ActiveRecord::Tenanted::DatabaseAdapters::SQLite.stub(:new, sqlite_mock) do
        ActiveRecord::Tenanted::DatabaseAdapter.drop_database(sqlite_config)
      end

      assert_mock sqlite_mock
    end

    test ".database_exists? calls SQLite#database_exists?" do
      sqlite_mock = Minitest::Mock.new
      sqlite_mock.expect(:database_exists?, true)

      result = ActiveRecord::Tenanted::DatabaseAdapters::SQLite.stub(:new, sqlite_mock) do
        ActiveRecord::Tenanted::DatabaseAdapter.database_exists?(sqlite_config)
      end

      assert_equal true, result
      assert_mock sqlite_mock
    end

    test ".list_tenant_databases calls SQLite#list_tenant_databases" do
      sqlite_mock = Minitest::Mock.new
      sqlite_mock.expect(:list_tenant_databases, [ "a", "b" ])

      result = ActiveRecord::Tenanted::DatabaseAdapters::SQLite.stub(:new, sqlite_mock) do
        ActiveRecord::Tenanted::DatabaseAdapter.list_tenant_databases(sqlite_config)
      end

      assert_equal [ "a", "b" ], result
      assert_mock sqlite_mock
    end

    test ".validate_tenant_name calls SQLite#validate_tenant_name" do
      sqlite_mock = Minitest::Mock.new
      sqlite_mock.expect(:validate_tenant_name, nil, [ "tenant1" ])

      ActiveRecord::Tenanted::DatabaseAdapters::SQLite.stub(:new, sqlite_mock) do
        ActiveRecord::Tenanted::DatabaseAdapter.validate_tenant_name(sqlite_config, "tenant1")
      end

      assert_mock sqlite_mock
    end

    test ".acquire_lock (new signature) calls SQLite#acquire_lock" do
      lock_name = "tenant_creation_test.sqlite3"
      sqlite_mock = Minitest::Mock.new
      sqlite_mock.expect(:acquire_lock, nil, [ lock_name ])

      ActiveRecord::Tenanted::DatabaseAdapters::SQLite.stub(:new, sqlite_mock) do
        ActiveRecord::Tenanted::DatabaseAdapter.acquire_lock(sqlite_config, lock_name) { }
      end

      assert_mock sqlite_mock
    end

    test ".acquire_lock (legacy signature) calls SQLite.acquire_lock (class)" do
      identifier = "legacy_lock_id"

      called = nil
      klass = ActiveRecord::Tenanted::DatabaseAdapters::SQLite
      existed = klass.respond_to?(:acquire_lock)
      klass.singleton_class.class_eval { define_method(:acquire_lock) { |*args, **kwargs| } } unless existed

      klass.stub(:acquire_lock, ->(id, &blk) { called = id; blk&.call }) do
        ActiveRecord::Tenanted::DatabaseAdapter.acquire_lock(identifier) { :ok }
      end

      assert_equal identifier, called
    ensure
      klass.singleton_class.send(:remove_method, :acquire_lock) unless existed
    end

    test ".acquire_lock (new signature, non-sqlite) yields without calling adapter" do
      non_sqlite_config = create_config("postgresql", "db_name")

      yielded = false
      ActiveRecord::Tenanted::DatabaseAdapter.acquire_lock(non_sqlite_config, "ignored") { yielded = true }

      assert_equal true, yielded
    end
  end


  private
    def create_config(adapter, database)
      config_hash = {
        adapter: adapter,
        database: database,
      }

      ActiveRecord::DatabaseConfigurations::HashConfig.new(
        "test",
        "test_config",
        config_hash
      )
    end
end
