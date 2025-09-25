# frozen_string_literal: true

require "test_helper"

describe ActiveRecord::Tenanted::DatabaseAdapter do
  describe ".adapter_for" do
    test "selects correct adapter for sqlite3" do
      adapter = ActiveRecord::Tenanted::DatabaseAdapter.adapter_for(create_config("sqlite3"))
      assert_instance_of ActiveRecord::Tenanted::DatabaseAdapters::SQLite, adapter
    end

    test "raises error for unsupported adapter" do
      unsupported_config = create_config("mongodb")

      error = assert_raises ActiveRecord::Tenanted::Error do
        ActiveRecord::Tenanted::DatabaseAdapter.adapter_for(unsupported_config)
      end

      assert_includes error.message, "Unsupported database adapter for tenanting: mongodb. Supported adapters: sqlite3"
    end
  end

  describe "delegation" do
    ActiveRecord::Tenanted::DatabaseAdapter::ADAPTERS.each do |adapter, adapter_class_name|
      test ".create_database calls adapter's #create_database" do
        adapter_mock = Minitest::Mock.new
        adapter_mock.expect(:create_database, nil)

        "#{adapter_class_name}".constantize.stub(:new, adapter_mock) do
          ActiveRecord::Tenanted::DatabaseAdapter.create_database(create_config(adapter))
        end

        assert_mock adapter_mock
      end

      test ".drop_database calls adapter's #drop_database" do
        adapter_mock = Minitest::Mock.new
        adapter_mock.expect(:drop_database, nil)

        "#{adapter_class_name}".constantize.stub(:new, adapter_mock) do
          ActiveRecord::Tenanted::DatabaseAdapter.drop_database(create_config(adapter))
        end

        assert_mock adapter_mock
      end

      test ".database_exists? calls adapter's #database_exists?" do
        adapter_mock = Minitest::Mock.new
        adapter_mock.expect(:database_exists?, true)

        result = "#{adapter_class_name}".constantize.stub(:new, adapter_mock) do
          ActiveRecord::Tenanted::DatabaseAdapter.database_exists?(create_config(adapter))
        end

        assert_equal true, result
        assert_mock adapter_mock
      end

      test ".list_tenant_databases calls adapter's #list_tenant_databases" do
        adapter_mock = Minitest::Mock.new
        adapter_mock.expect(:list_tenant_databases, [ "foo", "bar" ])

        result = "#{adapter_class_name}".constantize.stub(:new, adapter_mock) do
          ActiveRecord::Tenanted::DatabaseAdapter.list_tenant_databases(create_config(adapter))
        end

        assert_equal [ "foo", "bar" ], result
        assert_mock adapter_mock
      end

      test ".validate_tenant_name calls adapter's #validate_tenant_name" do
        adapter_mock = Minitest::Mock.new
        adapter_mock.expect(:validate_tenant_name, nil, [ "tenant1" ])

        "#{adapter_class_name}".constantize.stub(:new, adapter_mock) do
          ActiveRecord::Tenanted::DatabaseAdapter.validate_tenant_name(create_config(adapter), "tenant1")
        end

        assert_mock adapter_mock
      end

      test ".acquire_lock (new signature) calls adapter's #acquire_lock" do
        lock_name = "tenant_creation_test.sqlite3"
        adapter_mock = Minitest::Mock.new
        adapter_mock.expect(:acquire_lock, nil, [ lock_name ])

        "#{adapter_class_name}".constantize.stub(:new, adapter_mock) do
          ActiveRecord::Tenanted::DatabaseAdapter.acquire_lock(create_config(adapter), lock_name) { }
        end

        assert_mock adapter_mock
      end

      test ".acquire_lock (legacy signature) calls adapter's #acquire_lock (class)" do
        identifier = "legacy_lock_id"

        called = nil
        klass = "#{adapter_class_name}".constantize
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
        non_sqlite_config = create_config("mysql")

        yielded = false
        ActiveRecord::Tenanted::DatabaseAdapter.acquire_lock(non_sqlite_config, "ignored") { yielded = true }

        assert_equal true, yielded
      end
    end
  end

  private
    def create_config(adapter)
      ActiveRecord::DatabaseConfigurations::HashConfig.new(
        "test",
        "test_config",
        {
          adapter: adapter,
          database: "db_name",
        }
      )
    end
end
