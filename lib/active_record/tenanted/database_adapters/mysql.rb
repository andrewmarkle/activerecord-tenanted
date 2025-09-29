# frozen_string_literal: true

module ActiveRecord
  module Tenanted
    module DatabaseAdapters
      class MySQL
        def initialize(db_config)
          @db_config = db_config
          @configuration_hash = db_config.configuration_hash
        end

        def create_database
          database_name = db_config.database

          # Create a temporary config without the specific database to connect to MySQL server
          server_config = db_config.configuration_hash.dup
          server_config.delete(:database)
          temp_config = ActiveRecord::DatabaseConfigurations::HashConfig.new(
            db_config.env_name,
            "#{db_config.name}_server",
            server_config
          )

          ActiveRecord::Tasks::DatabaseTasks.with_temporary_connection(temp_config) do |conn|
            # Use ActiveRecord's built-in create_database method with charset/collation from config
            create_options = {}

            # Add charset/encoding if specified
            if charset = db_config.configuration_hash[:encoding] || db_config.configuration_hash[:charset]
              create_options[:charset] = charset
            end

            # Add collation if specified
            if collation = db_config.configuration_hash[:collation]
              create_options[:collation] = collation
            end

            conn.create_database(database_name, create_options)
          end
        end

        def drop_database
          database_name = db_config.database

          # Create a temporary config without the specific database to connect to MySQL server
          server_config = db_config.configuration_hash.dup
          server_config.delete(:database)
          temp_config = ActiveRecord::DatabaseConfigurations::HashConfig.new(
            db_config.env_name,
            "#{db_config.name}_server",
            server_config
          )

          ActiveRecord::Tasks::DatabaseTasks.with_temporary_connection(temp_config) do |conn|
            # Use ActiveRecord's built-in drop_database method
            conn.drop_database(database_name)
          end
        end

        def database_exists?(arguments = {})
          database_name = db_config.database

          # Create a temporary config without the specific database to connect to MySQL server
          server_config = db_config.configuration_hash.dup
          server_config.delete(:database)
          temp_config = ActiveRecord::DatabaseConfigurations::HashConfig.new(
            db_config.env_name,
            "#{db_config.name}_server",
            server_config
          )

          ActiveRecord::Tasks::DatabaseTasks.with_temporary_connection(temp_config) do |conn|
            result = conn.execute("SHOW DATABASES LIKE '#{database_name}'")
            result.any?
          end
        rescue ActiveRecord::NoDatabaseError, Mysql2::Error
          false
        end

        def list_tenant_databases
          # Extract the database pattern from the root config
          # e.g., "myapp_%{tenant}" becomes "myapp_%"
          database_pattern = db_config.database.gsub(/%\{tenant\}/, "%")

          # Create a temporary config without the specific database to connect to MySQL server
          server_config = db_config.configuration_hash.dup
          server_config.delete(:database)
          temp_config = ActiveRecord::DatabaseConfigurations::HashConfig.new(
            db_config.env_name,
            "#{db_config.name}_server",
            server_config
          )

          ActiveRecord::Tasks::DatabaseTasks.with_temporary_connection(temp_config) do |conn|
            result = conn.execute("SHOW DATABASES LIKE '#{database_pattern}'")

            # Extract tenant names from database names
            # e.g., "myapp_tenant1" -> "tenant1"
            prefix_pattern = db_config.database.split("%{tenant}").first
            suffix_pattern = db_config.database.split("%{tenant}").last

            result.filter_map do |row|
              db_name = row[0] || row.first
              # Remove prefix and suffix to get tenant name
              tenant_name = db_name.dup
              tenant_name = tenant_name.sub(/^#{Regexp.escape(prefix_pattern)}/, "") if prefix_pattern.present?
              tenant_name = tenant_name.sub(/#{Regexp.escape(suffix_pattern)}$/, "") if suffix_pattern.present?
              tenant_name
            end.reject(&:empty?)
          end
        rescue ActiveRecord::NoDatabaseError, Mysql2::Error => e
          Rails.logger.warn "Could not list tenant databases: #{e.message}"
          []
        end

        def acquire_lock(db_config, &block)
          yield
        end

        def validate_tenant_name(tenant_name)
          if tenant_name.length > 64
            raise ActiveRecord::Tenanted::BadTenantNameError, "Tenant name too long (max 64 characters): #{tenant_name.inspect}"
          end

          if tenant_name.match?(/[^a-zA-Z0-9_$-]/)
            raise ActiveRecord::Tenanted::BadTenantNameError, "Tenant name contains invalid characters (only letters, numbers, underscore, and $ allowed): #{tenant_name.inspect}"
          end

          if tenant_name.match?(/^\d/)
            raise ActiveRecord::Tenanted::BadTenantNameError, "Tenant name cannot start with a number: #{tenant_name.inspect}"
          end

          reserved_words = %w[
            database databases table tables column columns index indexes
            select insert update delete create drop alter
            user users group groups order by from where
            and or not null true false
          ]

          if reserved_words.include?(tenant_name.downcase)
            raise ActiveRecord::Tenanted::BadTenantNameError, "Tenant name is a reserved MySQL keyword: #{tenant_name.inspect}"
          end
        end


        private
          attr_reader :db_config, :db_configuration_hash


          def default_server_config
            primary_config = ActiveRecord::Base.configurations.configs_for(
              env_name: Rails.env,
              include_hidden: true
            ).find { |c| c.name == "primary" || c.primary? }

            if primary_config
              config_hash = primary_config.configuration_hash.dup
              config_hash.delete(:database)
              config_hash
            else
              {
                adapter: "mysql2",
                host: "localhost",
                username: "root",
              }
            end
          end
      end
    end
  end
end
