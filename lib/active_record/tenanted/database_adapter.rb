# frozen_string_literal: true

module ActiveRecord
  module Tenanted
    class DatabaseAdapter
      ADAPTERS = {
        "sqlite3" => "ActiveRecord::Tenanted::DatabaseAdapters::SQLite",
      }.freeze

      class << self
        def create_database(db_config)
          adapter_for(db_config).create_database
        end

        def drop_database(db_config)
          adapter_for(db_config).drop_database
        end

        def database_exists?(db_config)
          adapter_for(db_config).database_exists?
        end

        def acquire_lock(db_config_or_identifier, lock_name = nil, &block)
          # Handle both new signature (config, lock_name) and legacy (identifier)
          if db_config_or_identifier.respond_to?(:adapter)
            config = db_config_or_identifier
            adapter_name = config.adapter || config.configuration_hash[:adapter]

            if adapter_name == "sqlite3"
              identifier = lock_name || "tenant_creation_#{config.database}"
              adapter_for(config).acquire_lock(identifier, &block)
            else
              # MySQL and other databases don't need locking - just execute the block
              yield
            end
          else
            identifier = config_or_identifier
            ActiveRecord::Tenanted::DatabaseAdapters::SQLite.acquire_lock(identifier, &block)
          end
        end

        def list_tenant_databases(db_config)
          adapter_for(db_config).list_tenant_databases
        end

        def validate_tenant_name(db_config, tenant_name)
          adapter_for(db_config).validate_tenant_name(tenant_name)
        end

        def adapter_for(db_config, *arguments)
          adapter_name = db_config.adapter || db_config.configuration_hash[:adapter]
          adapter_class_name = ADAPTERS[adapter_name]

          if adapter_class_name.nil?
            raise ActiveRecord::Tenanted::Error,
                  "Unsupported database adapter for tenanting: #{adapter_name}. " \
                  "Supported adapters: #{ADAPTERS.keys.join(', ')}"
          end

          adapter_class_name.constantize.new(db_config, *arguments)
        end
      end
    end
  end
end
