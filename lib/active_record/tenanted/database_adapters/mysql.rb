# frozen_string_literal: true

module ActiveRecord
  module Tenanted
    module DatabaseAdapters
      class MySQL
        attr_reader :db_config

        def initialize(db_config)
          @db_config = db_config
        end

        def tenant_databases
          like_pattern = db_config.database.gsub(/%\{tenant\}/, "%")
          scanner = Regexp.new("^" + Regexp.escape(db_config.database).gsub(Regexp.escape("%{tenant}"), "(.+)") + "$")

          ActiveRecord::Tasks::DatabaseTasks.with_temporary_connection(configuration_hash_without_database) do |connection|
            result = connection.execute("SHOW DATABASES LIKE '#{like_pattern}'")

            result.filter_map do |row|
              db_name = row[0] || row.first
              match = db_name.match(scanner)
              if match.nil?
                Rails.logger.warn "ActiveRecord::Tenanted: Cannot parse tenant name from database #{db_name.inspect}"
                nil
              else
                match[1]
              end
            end
          end
        rescue ActiveRecord::NoDatabaseError, Mysql2::Error => e
          Rails.logger.warn "Could not list tenant databases: #{e.message}"
          []
        end

        def validate_tenant_name(tenant_name)
          tenant_name_str = tenant_name.to_s

          database_name = sprintf(db_config.database, tenant: tenant_name_str)

          return if database_name.include?("%{") || database_name.include?("%}")

          if database_name.length > 64
            raise ActiveRecord::Tenanted::BadTenantNameError, "Database name too long (max 64 characters): #{database_name.inspect}"
          end

          if database_name.match?(/[^a-zA-Z0-9_$-]/)
            raise ActiveRecord::Tenanted::BadTenantNameError, "Database name contains invalid characters (only letters, numbers, underscore, $ and hyphen allowed): #{database_name.inspect}"
          end

          if database_name.match?(/^\d/)
            raise ActiveRecord::Tenanted::BadTenantNameError, "Database name cannot start with a number: #{database_name.inspect}"
          end

          reserved_words = %w[
            database databases table tables column columns index indexes
            select insert update delete create drop alter
            user users group groups order by from where
            and or not null true false
          ]

          if reserved_words.include?(database_name.downcase)
            raise ActiveRecord::Tenanted::BadTenantNameError, "Database name is a reserved MySQL keyword: #{database_name.inspect}"
          end
        end

        def create_database
          ActiveRecord::Tasks::DatabaseTasks.with_temporary_connection(configuration_hash_without_database) do |connection|
            create_options = Hash.new.tap do |options|
              options[:charset] = db_config.configuration_hash[:encoding]   if db_config.configuration_hash.include?(:encoding)
              options[:collation] = db_config.configuration_hash[:collation]  if db_config.configuration_hash.include?(:collation)
            end

            connection.create_database(database_path, create_options)
          end
        end

        def drop_database
          ActiveRecord::Tasks::DatabaseTasks.with_temporary_connection(configuration_hash_without_database) do |connection|
            connection.drop_database(database_path)
          end
        end

        def database_exist?
          ActiveRecord::Tasks::DatabaseTasks.with_temporary_connection(configuration_hash_without_database) do |connection|
            result = connection.execute("SHOW DATABASES LIKE '#{database_path}'")
            result.any?
          end
        rescue ActiveRecord::NoDatabaseError, Mysql2::Error
          false
        end

        def database_ready?
          database_exist?
        end

        def acquire_ready_lock(&block)
          yield
        end

        def ensure_database_directory_exists
          database_path.present?
        end

        def database_path
          db_config.database
        end

        def test_workerize(db, test_worker_id)
          test_worker_suffix = "_#{test_worker_id}"

          if db.end_with?(test_worker_suffix)
            db
          else
            db + test_worker_suffix
          end
        end

        def path_for(database)
          database
        end

      private
        def configuration_hash_without_database
          configuration_hash = db_config.configuration_hash.dup.merge(database: nil)
          ActiveRecord::DatabaseConfigurations::HashConfig.new(
            db_config.env_name,
            db_config.name.to_s,
            configuration_hash
          )
        end
      end
    end
  end
end
