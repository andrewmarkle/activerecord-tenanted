# frozen_string_literal: true

require "rake"

module ActiveRecord
  module Tenanted
    class DatabaseTasks # :nodoc:
      include Rake::DSL

      class << self
        def verbose?
          ActiveRecord::Tasks::DatabaseTasks.send(:verbose?)
        end
      end

      attr_reader :config

      def initialize(config)
        unless config.is_a?(ActiveRecord::Tenanted::DatabaseConfigurations::BaseConfig)
          raise TypeError, "Argument must be an instance of ActiveRecord::Tenanted::DatabaseConfigurations::BaseConfig"
        end
        @config = config
      end

      def migrate_all
        tenants = config.tenants.presence || [ get_default_tenant ].compact
        tenants.each do |tenant|
          migrate_tenant(tenant)
        end
      end

      def migrate_tenant(tenant = set_current_tenant)
        db_config = config.new_tenant_config(tenant)
        migrate(db_config)
        $stdout.puts "Migrated database '#{db_config.database}'" if verbose?
      end

      def drop_all
        config.tenants.each do |tenant|
          drop_tenant(tenant)
        end
      end

      def drop_tenant(tenant = set_current_tenant)
        db_config = config.new_tenant_config(tenant)
        db_config.config_adapter.drop_database
        $stdout.puts "Dropped database '#{db_config.database}'" if verbose?
      end

      def get_default_tenant
        # TODO: needs to work with multiple tenanted configs, maybe using ENV["ARTENANT_#{config.name}"]
        tenant = ENV["ARTENANT"]

        if tenant.present?
          $stdout.puts "Setting current tenant to #{tenant.inspect}" if verbose?
        elsif Rails.env.local?
          tenant = Rails.application.config.active_record_tenanted.default_tenant
          $stdout.puts "Defaulting current tenant for #{config.name.inspect} to #{tenant.inspect}" if verbose?
        else
          tenant = nil
          $stdout.puts "Cannot determine an implicit tenant: ARTENANT not set, and Rails.env is not local." if verbose?
        end

        tenant
      end

      def set_current_tenant
        unless (connection_class = ActiveRecord::Tenanted.connection_class)
          raise ActiveRecord::Tenanted::IntegrationNotConfiguredError,
                "ActiveRecord::Tenanted integration is not configured via connection_class"
        end

        if connection_class.current_tenant.nil?
          connection_class.current_tenant = get_default_tenant
        else
          connection_class.current_tenant
        end
      end

      # This is essentially a simplified implementation of ActiveRecord::Tasks::DatabaseTasks.migrate
      def migrate(config)
        ActiveRecord::Tasks::DatabaseTasks.with_temporary_connection(config) do |conn|
          pool = conn.pool

          # initialize_database
          unless pool.schema_migration.table_exists?
            schema_dump_path = ActiveRecord::Tasks::DatabaseTasks.schema_dump_path(config)
            if schema_dump_path && File.exist?(schema_dump_path)
              ActiveRecord::Tasks::DatabaseTasks.load_schema(config)
            end
            # TODO: emit a "Created database" message once we sort out implicit creation
          end

          # migrate
          migrated = false
          if pool.migration_context.pending_migration_versions.present?
            pool.migration_context.migrate(nil)
            pool.schema_cache.clear!
            migrated = true
          end

          # dump the schema and schema cache
          if Rails.env.development? || ENV["ARTENANT_SCHEMA_DUMP"].present?
            if migrated
              ActiveRecord::Tasks::DatabaseTasks.dump_schema(config)
            end

            cache_dump = ActiveRecord::Tasks::DatabaseTasks.cache_dump_filename(config)
            if migrated || !File.exist?(cache_dump)
              ActiveRecord::Tasks::DatabaseTasks.dump_schema_cache(pool, cache_dump)
            end
          end
        end
      end

      def verbose?
        self.class.verbose?
      end

      def register_rake_tasks
        name = config.name

        desc "Migrate tenanted #{name} databases for current environment"
        task "db:migrate:#{name}" => "load_config" do
          verbose_was = ActiveRecord::Migration.verbose
          ActiveRecord::Migration.verbose = ActiveRecord::Tenanted::DatabaseTasks.verbose?

          tenant = ENV["ARTENANT"]
          if tenant.present?
            migrate_tenant(tenant)
          else
            migrate_all
          end
        ensure
          ActiveRecord::Migration.verbose = verbose_was
        end
        task "db:migrate" => "db:migrate:#{name}"
        task "db:prepare" => "db:migrate:#{name}"

        desc "Drop tenanted #{name} databases for current environment"
        task "db:drop:#{name}" => "load_config" do
          verbose_was = ActiveRecord::Migration.verbose
          ActiveRecord::Migration.verbose = ActiveRecord::Tenanted::DatabaseTasks.verbose?

          tenant = ENV["ARTENANT"]
          if tenant.present?
            drop_tenant(tenant)
          else
            drop_all
          end
        ensure
          ActiveRecord::Migration.verbose = verbose_was
        end
        task "db:drop" => "db:drop:#{name}"

        # TODO: Rails' database tasks include "db:seed" in the tasks that "db:reset" runs.
        desc "Drop and recreate tenanted #{name} database from its schema for the current environment"
        task "db:reset:#{name}" => [ "db:drop:#{name}", "db:migrate:#{name}" ]
        task "db:reset" => "db:reset:#{name}"
      end
    end
  end
end
