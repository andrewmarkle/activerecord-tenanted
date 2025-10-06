# frozen_string_literal: true

module ActiveRecord
  module Tenanted
    module DatabaseAdapters
      class SQLite # :nodoc:
        def initialize(db_config)
          @db_config = db_config
          @database_path = coerce_path(db_config.database)
        end

        def create_database
          # Ensure the directory exists
          database_dir = File.dirname(database_path)
          FileUtils.mkdir_p(database_dir) unless File.directory?(database_dir)

          # Create the SQLite database file
          FileUtils.touch(database_path)
        end

        def drop_database
          # Remove the SQLite database file and associated files
          FileUtils.rm_f(database_path)
          FileUtils.rm_f("#{database_path}-wal")  # Write-Ahead Logging file
          FileUtils.rm_f("#{database_path}-shm")  # Shared Memory file
        end

        def database_exist?
          File.exist?(database_path)
        end

        def database_ready?
          File.exist?(database_path) && !ActiveRecord::Tenanted::Mutex::Ready.locked?(database_path)
        end

        def tenant_databases
          glob_database = db_config.database_for("*")
          glob = coerce_path(glob_database)

          scanner_database = db_config.database_for("(.+)")
          scanner = Regexp.new(coerce_path(scanner_database))

          Dir.glob(glob).filter_map do |path|
            result = path.scan(scanner).flatten.first
            if result.nil?
              Rails.logger.warn "ActiveRecord::Tenanted: Cannot parse tenant name from filename #{path.inspect}"
            end
            result
          end
        end

        def acquire_ready_lock(db_config, &block)
          ActiveRecord::Tenanted::Mutex::Ready.lock(database_path, &block)
        end

        def validate_tenant_name(tenant_name)
          if tenant_name.match?(%r{[/'"`]})
            raise BadTenantNameError, "Tenant name contains an invalid character: #{tenant_name.inspect}"
          end
        end

        attr_reader :database_path

        private
          attr_reader :db_config

          def coerce_path(path)
            return path unless path.start_with?("file:")
            # Paths with %{tenant} are not valid URI paths and we don't need to coerce them.
            return path if path.include?("%{")

            if path.start_with?("file:/")
              URI.parse(path).path
            else
              URI.parse(path.sub(/\?.*$/, "")).opaque
            end
          end
      end
    end
  end
end
